"""本地配置分发控制面：依赖闭包、逐代理生效与连接钉住。

资源以 (name, rev) 定位，kind 为 listener / route / cluster，refs 指到确定的
(name, rev)。监听器引用路由、路由引用集群，构成一条引用链；网络差时链上
各种资源可能相隔很久才到齐。本服务把三件事解释清楚：

* 配置推送：按每个代理的订阅与既知资源，只补充实际需要的改动；
* 代理报告：代理能否激活一套候选配置（确认 / 拒绝 / 切换反馈）；
* 连接使用：现有连接仍依赖哪些旧版本，旧资源何时确实可以删除。

语义约定
--------

* 候选：每个被订阅监听器取仓库中该名的最高 listener 修订。候选快照里
  有缺失引用、循环依赖或同名不相容修订时不得给代理说已能切流，控制面
  也不会把该候选列入推送计划，代理保持原来可用的配置。
* 既知（have）：代理当前代次申报持有的版本，加上已接受批次送达的资源。
  被拒绝的批次整体不计入；重连时代理重新申报，旧代次的确认一律作废，
  不能算它认可了新候选。
* 激活：候选闭包已完整被代理持有、候选未被拒绝、且无在途批次覆盖时，
  才接受 activate；否则代理继续用旧配置。确认收到某份路由不等于流量
  已切换——路由仍可能引用一个代理没有的集群。
* 连接：opened 时钉住当时生效监听器修订的闭包，closed 时释放。切换前
  建立的连接保留它自己实际所需的旧引用直至结束；新连接使用已兑现的
  新配置。
* 删除：remove(hard=False) 登记删除意向，无人需要时立即落实，否则延期
  到依赖消失（断连站保留的旧版本、在线站待补的依赖都算依赖）。同一
  资源被两处引用时，一处迁走不构成删除条件。remove(hard=True) 强制
  停用确实不可继续供应的资源，报告仍停在该资源上的监听器与连接；
  不可激活的新候选不能当作替代。
* restart：控制面重启只作废在途批次；仓库、订阅、激活、连接与删除
  意向全部保留。
"""

from __future__ import annotations

from dataclasses import dataclass, field

LISTENER = "listener"

# 候选状态取值：
#   no-published-rev  仓库还没有该监听器的任何修订
#   blocked           候选快照无效（缺失引用 / 循环依赖 / 不相容修订）
#   awaiting_delivery 快照有效，代理还缺引用，等待下发
#   sent              差额已在在途批次中，等待代理确认
#   rejected          代理拒绝了该候选涉及的资源，保持原配置
#   ready             代理已收齐完整引用，等待 activate 切换
#   serving           已激活，正在服务


def _key(pair):
    """把 [name, rev] 形式的输入规整为 (str, int) 元组。"""
    name, rev = pair
    return str(name), int(rev)


def _fmt(keys):
    """把资源键集合排成稳定的 [[name, rev], ...] 列表，便于输出与比较。"""
    return [list(key) for key in sorted(keys)]


@dataclass
class Resource:
    name: str
    rev: int
    kind: str
    refs: tuple
    remove_requested: bool = False  # 运维已申请删除，等待依赖消失


@dataclass
class Nonce:
    resources: frozenset
    removes: frozenset
    status: str = "pending"  # pending / accepted / rejected / orphaned
    rejected_resource: tuple | None = None


@dataclass
class Connection:
    proxy: str
    listener: str
    pins: frozenset  # 建立时钉住的闭包（含被强制停用但仍实际所需的引用）


@dataclass
class ProxyState:
    name: str
    epoch: int
    listeners: tuple = ()
    have: set = field(default_factory=set)
    connected: bool = True
    serving: dict = field(default_factory=dict)    # 监听器名 -> (name, rev)，跨代次保留
    nonces: dict = field(default_factory=dict)     # nonce -> Nonce，限当前代次
    rejected: dict = field(default_factory=dict)   # 监听器名 -> {"rev", "resource"}，限当前代次


class ControlPlane:
    """配置分发控制面。所有变更方法返回 JSON 友好的 dict，说明决策结果。"""

    def __init__(self):
        self._repo = {}              # (name, rev) -> Resource
        self._proxies = {}           # name -> ProxyState
        self._connections = {}       # conn -> Connection
        self._hard_removed = set()   # 被强制停用、外部不再供应的资源
        self._collected = set()      # 延期删除已落实的资源
        self._notices = []           # 本次变更连带产生的通知（如延期删除落实）

    # ------------------------------------------------------------------
    # 内部：闭包与候选评估
    # ------------------------------------------------------------------

    def _closure(self, root):
        """从 root 出发的引用闭包，同时检出缺失、循环与同名不相容修订。"""
        resources, missing, forced = set(), set(), set()
        conflicts, cycles = {}, []
        seen, stack = {}, []

        def visit(key):
            if key in stack:
                cycles.append([*stack[stack.index(key):], key])
                return
            if key not in self._repo:
                missing.add(key)
                if key in self._hard_removed:
                    forced.add(key)
                return
            name, rev = key
            if name in seen:
                if seen[name] != rev:
                    conflicts.setdefault(name, set()).update((seen[name], rev))
                return
            seen[name] = rev
            resources.add(key)
            stack.append(key)
            for ref in self._repo[key].refs:
                visit(ref)
            stack.pop()

        visit(root)
        return {
            "ok": not missing and not cycles and not conflicts,
            "resources": resources,
            "missing": missing,
            "forced": forced,
            "conflicts": conflicts,
            "cycles": cycles,
        }

    def _candidate_view(self, state, listener):
        """某代理某监听器的当前视图：仍在哪路由、候选是谁、卡在哪一步。"""
        serving = state.serving.get(listener)
        view = {"listener": listener, "serving": list(serving) if serving else None}
        if serving is not None:
            closure = self._closure(serving)
            view["still_routes_via"] = _fmt(closure["resources"])
            if closure["missing"]:
                view["serving_degraded"] = {
                    "missing": _fmt(closure["missing"]),
                    "force_disabled": _fmt(closure["forced"]),
                }
        revs = [rev for (name, rev), res in self._repo.items()
                if name == listener and res.kind == LISTENER]
        if not revs:
            view["status"] = "no-published-rev"
            return view
        latest = max(revs)
        candidate = (listener, latest)
        view["candidate"] = list(candidate)
        if serving == candidate:
            view["status"] = "serving"
            return view
        closure = self._closure(candidate)
        if not closure["ok"]:
            view["status"] = "blocked"
            reasons = {}
            if closure["missing"]:
                reasons["missing"] = _fmt(closure["missing"])
                if closure["forced"]:
                    reasons["force_disabled"] = _fmt(closure["forced"])
            if closure["cycles"]:
                reasons["cycles"] = [[list(k) for k in path] for path in closure["cycles"]]
            if closure["conflicts"]:
                reasons["incompatible_revs"] = {
                    name: sorted(revs) for name, revs in closure["conflicts"].items()
                }
            view["reasons"] = reasons
            return view
        rejected = state.rejected.get(listener)
        if rejected and rejected["rev"] == latest:
            view["status"] = "rejected"
            view["rejected_resource"] = (
                list(rejected["resource"]) if rejected["resource"] else None
            )
            view["note"] = "代理保持原可用配置；更高的监听器修订或新代次可解除"
            return view
        missing = closure["resources"] - state.have
        if missing:
            inflight = set().union(
                *(batch.resources for batch in state.nonces.values()
                  if batch.status == "pending")
            )
            todo = missing - inflight
            view["missing"] = _fmt(missing)
            view["status"] = "awaiting_delivery" if todo else "sent"
            if todo:
                view["to_send"] = _fmt(todo)
        else:
            view["status"] = "ready"
        return view

    def _needed_by_proxy(self, state):
        """代理侧仍需要的资源：生效配置、有效候选与连接钉住的并集。"""
        needed = set()
        for serving in state.serving.values():
            closure = self._closure(serving)
            # 降级配置仍实际引用被强制停用的资源，不能算可清理
            needed |= closure["resources"] | closure["missing"]
        for listener in state.listeners:
            view = self._candidate_view(state, listener)
            candidate = view.get("candidate")
            if candidate and view["status"] in {"awaiting_delivery", "sent", "ready"}:
                needed |= self._closure(tuple(candidate))["resources"]
        for conn in self._connections.values():
            if conn.proxy == state.name:
                needed |= conn.pins
        return needed

    def _dependents(self, key):
        """仓库侧谁还需要该资源：生效配置、待补候选、打开的连接。"""
        serving, candidates, connections = [], [], []
        for state in self._proxies.values():
            for listener, skey in state.serving.items():
                closure = self._closure(skey)
                if key in closure["resources"] or key in closure["missing"]:
                    serving.append(
                        {"proxy": state.name, "listener": listener, "rev": skey[1]}
                    )
            for listener in state.listeners:
                view = self._candidate_view(state, listener)
                candidate = view.get("candidate")
                if candidate and view["status"] in {"awaiting_delivery", "sent", "ready"}:
                    if key in self._closure(tuple(candidate))["resources"]:
                        candidates.append(
                            {"proxy": state.name, "listener": listener,
                             "rev": candidate[1]}
                        )
        for cid, conn in self._connections.items():
            if key in conn.pins:
                connections.append(
                    {"conn": cid, "proxy": conn.proxy, "listener": conn.listener}
                )
        return {"serving": serving, "candidates": candidates,
                "connections": connections}

    def _blocked_candidates_on(self, key):
        """因缺少 key 而不可激活的候选（用于强制停用后的影响说明）。"""
        blocked = []
        for state in self._proxies.values():
            for listener in state.listeners:
                view = self._candidate_view(state, listener)
                if view.get("status") != "blocked":
                    continue
                missing = {
                    tuple(pair) for pair in view.get("reasons", {}).get("missing", [])
                }
                if key in missing:
                    blocked.append({"proxy": state.name, "listener": listener,
                                    "candidate": view.get("candidate")})
        return blocked

    def _collect_garbage(self):
        """延期删除的资源和已无人需要的资源，在依赖消失时落实删除。"""
        for key, res in list(self._repo.items()):
            if not res.remove_requested:
                continue
            dependents = self._dependents(key)
            if not any(dependents.values()):
                del self._repo[key]
                self._collected.add(key)
                self._notices.append({
                    "notice": "resource-collected",
                    "resource": list(key),
                    "note": "已无人需要，删除落实",
                })

    def _finish(self, response):
        """每个变更操作的收尾：先做回收，再把连带通知挂到响应上。"""
        self._collect_garbage()
        if self._notices:
            response["notices"] = self._notices
            self._notices = []
        return response

    # ------------------------------------------------------------------
    # 配置推送接口
    # ------------------------------------------------------------------

    def publish(self, name, rev, kind, refs):
        """运维提交一份资源修订。同 (name, rev) 重复发布需内容一致。"""
        key = (str(name), int(rev))
        if key[1] <= 0:
            return self._finish({"ok": False, "resource": list(key),
                                 "error": "rev 必须为正整数"})
        refs = tuple(_key(pair) for pair in refs)
        existing = self._repo.get(key)
        if existing is not None:
            if existing.kind == kind and existing.refs == refs:
                return self._finish({"ok": True, "resource": list(key),
                                     "status": "unchanged"})
            return self._finish({"ok": False, "resource": list(key),
                                 "error": "同名同修订已存在且内容不同"})
        revived = key in self._hard_removed or key in self._collected
        self._hard_removed.discard(key)
        self._collected.discard(key)
        self._repo[key] = Resource(key[0], key[1], str(kind), refs)
        if key[1] > 0 and kind == LISTENER:
            # 更高的监听器修订构成新候选，解除旧的拒绝粘性
            for state in self._proxies.values():
                rejected = state.rejected.get(key[0])
                if rejected and key[1] > rejected["rev"]:
                    del state.rejected[key[0]]
        response = {"ok": True, "resource": list(key), "status": "published"}
        if revived:
            response["note"] = "该资源此前被移除，现已重新供应"
        return self._finish(response)

    def request_remove(self, name, rev, hard=False):
        """申请删除 name/rev。hard=False 延期到无人需要；hard=True 强制停用。"""
        key = (str(name), int(rev))
        if hard:
            dependents = self._dependents(key)
            blocked = self._blocked_candidates_on(key)
            existed = self._repo.pop(key, None) is not None
            self._collected.discard(key)
            self._hard_removed.add(key)
            return self._finish({
                "ok": True,
                "resource": list(key),
                "status": "force-disabled",
                "existed": existed,
                "stuck": dependents,
                "candidates_now_blocked": blocked,
                "note": "外部不再供应；stuck 是仍停在该资源上的监听器与连接，"
                        "不可激活的新候选不能当作替代",
            })
        if key not in self._repo:
            if key in self._collected:
                return self._finish({"ok": True, "resource": list(key),
                                     "status": "already-collected"})
            if key in self._hard_removed:
                return self._finish({"ok": True, "resource": list(key),
                                     "status": "already-force-disabled"})
            return self._finish({"ok": False, "resource": list(key),
                                 "status": "unknown",
                                 "note": "仓库中没有该资源"})
        self._repo[key].remove_requested = True
        dependents = self._dependents(key)
        if not any(dependents.values()):
            del self._repo[key]
            self._collected.add(key)
            return self._finish({"ok": True, "resource": list(key),
                                 "status": "deleted",
                                 "note": "无人需要，删除立即落实"})
        return self._finish({"ok": True, "resource": list(key),
                             "status": "deferred",
                             "blockers": dependents,
                             "note": "仍被需要，删除延期到依赖消失"})

    def plan_push(self, proxy):
        """按订阅与既知资源计算该代理的增量：待补资源与可清理资源。"""
        state = self._proxies.get(str(proxy))
        if state is None:
            return {"ok": False, "error": "未知代理", "proxy": proxy}
        send, listeners = set(), {}
        for listener in state.listeners:
            view = self._candidate_view(state, listener)
            listeners[listener] = view
            send.update(tuple(pair) for pair in view.get("to_send", []))
        removals = state.have - self._needed_by_proxy(state)
        return {
            "ok": True,
            "proxy": state.name,
            "epoch": state.epoch,
            "connected": state.connected,
            "send": _fmt(send),
            "removes": _fmt(removals),
            "listeners": listeners,
        }

    def plan_all(self):
        """所有在线代理的推送计划（断连站保留状态但不主动推）。"""
        return {
            name: self.plan_push(name)
            for name, state in self._proxies.items()
            if state.connected
        }

    def note_send(self, proxy, epoch, nonce, resources, removes=()):
        """登记实际发给代理的一批资源与删除项，供后续确认对账。"""
        state = self._proxies.get(str(proxy))
        if state is None or state.epoch != int(epoch) or not state.connected:
            return self._finish({
                "ok": False, "proxy": proxy, "nonce": nonce, "status": "stale",
                "note": "代理不在该代次或已断连，批次不予登记",
            })
        nonce = str(nonce)
        if nonce in state.nonces:
            return self._finish({"ok": False, "proxy": proxy, "nonce": nonce,
                                 "status": "duplicate"})
        resources = frozenset(_key(pair) for pair in resources)
        removes = frozenset(_key(pair) for pair in removes)
        warnings = []
        unknown = resources - self._repo.keys()
        if unknown:
            warnings.append({"warning": "unknown-resources",
                             "resources": _fmt(unknown)})
        for key in resources:
            res = self._repo.get(key)
            if res is not None and res.kind == LISTENER and not self._closure(key)["ok"]:
                warnings.append({
                    "warning": "incomplete-candidate",
                    "listener": list(key),
                    "note": "候选快照无效，代理收到也无法切换",
                })
        state.nonces[nonce] = Nonce(resources, removes)
        response = {"ok": True, "proxy": proxy, "epoch": int(epoch),
                    "nonce": nonce, "status": "pending"}
        if warnings:
            response["warnings"] = warnings
        return self._finish(response)

    def restart(self):
        """控制面重启：在途批次作废，其余状态保留。"""
        orphaned = 0
        for state in self._proxies.values():
            for batch in state.nonces.values():
                if batch.status == "pending":
                    batch.status = "orphaned"
                    orphaned += 1
        return self._finish({
            "ok": True,
            "status": "restarted",
            "orphaned_nonces": orphaned,
            "note": "在途批次作废；仓库、订阅、激活、连接与删除意向保留",
        })

    # ------------------------------------------------------------------
    # 代理报告接口
    # ------------------------------------------------------------------

    def proxy_connected(self, name, epoch, listeners, have):
        """代理接入或重连。新代次按申报重置既知，旧批次与拒绝记录作废。"""
        name, epoch = str(name), int(epoch)
        state = self._proxies.get(name)
        if state is not None and epoch < state.epoch:
            return self._finish({
                "ok": False, "proxy": name, "epoch": epoch,
                "status": "stale-epoch",
                "note": "比当前代次旧的连接报告不予接受",
            })
        new_epoch = state is None or epoch > state.epoch
        if state is None:
            state = ProxyState(name, epoch)
            self._proxies[name] = state
        if new_epoch:
            state.nonces.clear()
            state.rejected.clear()
        state.epoch = epoch
        state.connected = True
        state.listeners = tuple(str(item) for item in listeners)
        state.have = {_key(pair) for pair in have}
        response = {
            "ok": True,
            "proxy": name,
            "epoch": epoch,
            "status": "connected" if new_epoch else "refreshed",
            "note": "既知资源按申报重置；旧代次批次与拒绝记录作废"
                    if new_epoch else "同代次刷新，批次状态保留",
        }
        unusable = state.have & self._hard_removed
        if unusable:
            response["warnings"] = [{
                "warning": "declared-force-disabled",
                "resources": _fmt(unusable),
                "note": "代理申报持有的资源已被强制停用，无法再从控制面补给",
            }]
        response["plan"] = self.plan_push(name)
        return self._finish(response)

    def proxy_disconnected(self, proxy, epoch):
        """代理断连：在途批次作废，已激活配置与连接保留。"""
        state = self._proxies.get(str(proxy))
        if state is None or state.epoch != int(epoch):
            return self._finish({"ok": False, "proxy": proxy, "status": "stale",
                                 "note": "代次不符或未知代理"})
        state.connected = False
        for batch in state.nonces.values():
            if batch.status == "pending":
                batch.status = "orphaned"
        return self._finish({
            "ok": True, "proxy": state.name, "epoch": state.epoch,
            "status": "disconnected",
            "note": "在途批次作废；已激活配置与连接保留，供重连后核对",
        })

    def proxy_ack(self, proxy, epoch, nonce, result, resource=None, reason=None):
        """代理对某批次的确认或拒绝。旧代次、未知或已了结的批次一律忽略。"""
        state = self._proxies.get(str(proxy))
        nonce = str(nonce)
        if state is None or state.epoch != int(epoch) or not state.connected:
            return self._finish({
                "ok": False, "proxy": proxy, "nonce": nonce,
                "status": "ignored-stale",
                "note": "旧代次或未知代理的确认，不能算它认可了新候选",
            })
        batch = state.nonces.get(nonce)
        if batch is None:
            return self._finish({"ok": False, "proxy": proxy, "nonce": nonce,
                                 "status": "ignored-unknown-nonce"})
        if batch.status != "pending":
            return self._finish({"ok": False, "proxy": proxy, "nonce": nonce,
                                 "status": "ignored-already-resolved",
                                 "previous": batch.status})
        if result == "accepted":
            batch.status = "accepted"
            state.have |= batch.resources
            state.have -= batch.removes
            return self._finish({"ok": True, "proxy": proxy, "nonce": nonce,
                                 "status": "accepted", "have": _fmt(state.have)})
        if result != "rejected":
            return self._finish({"ok": False, "proxy": proxy, "nonce": nonce,
                                 "status": "unknown-result", "result": result})
        batch.status = "rejected"
        if isinstance(resource, (list, tuple)):
            targets = {_key(resource)}
        elif resource is not None:
            targets = {key for key in batch.resources if key[0] == str(resource)}
            if not targets:
                targets = {key for key in self._repo if key[0] == str(resource)}
        else:
            targets = set(batch.resources)
        batch.rejected_resource = sorted(targets)[0] if targets else None
        # 定位受影响监听器：候选闭包用到被拒绝资源的那些
        affected = []
        for listener in state.listeners:
            view = self._candidate_view(state, listener)
            candidate = view.get("candidate")
            if not candidate or view["status"] not in {"awaiting_delivery", "sent", "ready"}:
                continue
            closure = self._closure(tuple(candidate))
            if closure["ok"] and closure["resources"] & targets:
                state.rejected[listener] = {
                    "rev": candidate[1],
                    "resource": batch.rejected_resource,
                }
                affected.append(listener)
        still = {}
        for listener in affected:
            serving = state.serving.get(listener)
            still[listener] = (
                {"serving": list(serving),
                 "routes_via": _fmt(self._closure(serving)["resources"])}
                if serving else {"serving": None, "routes_via": []}
            )
        response = {
            "ok": True, "proxy": proxy, "nonce": nonce, "status": "rejected",
            "rejected_resource": (list(batch.rejected_resource)
                                  if batch.rejected_resource else None),
            "affected_listeners": affected,
            "still_routes": still,
            "note": "彼此无引用的其它监听器不受影响，继续更新",
        }
        if reason:
            response["reason"] = str(reason)
        return self._finish(response)

    def proxy_activated(self, proxy, epoch, name, rev):
        """代理反馈候选切换。快照无效、引用未收齐或批次未确认时不予接受。"""
        state = self._proxies.get(str(proxy))
        key = (str(name), int(rev))
        if state is None or state.epoch != int(epoch) or not state.connected:
            return self._finish({"ok": False, "proxy": proxy, "resource": list(key),
                                 "status": "ignored-stale"})
        closure = self._closure(key)
        if not closure["ok"]:
            return self._finish({
                "ok": False, "proxy": proxy, "resource": list(key),
                "status": "refused-invalid-candidate",
                "note": "候选快照有缺失引用、循环依赖或不相容修订，不能切流",
            })
        rejected = state.rejected.get(key[0])
        if rejected and rejected["rev"] == key[1]:
            return self._finish({
                "ok": False, "proxy": proxy, "resource": list(key),
                "status": "refused-rejected-candidate",
                "note": "该候选已被代理拒绝，保持原配置",
            })
        missing = closure["resources"] - state.have
        if missing:
            return self._finish({
                "ok": False, "proxy": proxy, "resource": list(key),
                "status": "refused-missing-refs",
                "missing": _fmt(missing),
                "note": "引用未收齐，代理保持原可用配置",
            })
        inflight = [nonce for nonce, batch in state.nonces.items()
                    if batch.status == "pending" and key in batch.resources]
        if inflight:
            return self._finish({
                "ok": False, "proxy": proxy, "resource": list(key),
                "status": "refused-awaiting-ack",
                "nonces": inflight,
                "note": "对应批次尚未被代理确认",
            })
        previous = state.serving.get(key[0])
        state.serving[key[0]] = key
        return self._finish({
            "ok": True, "proxy": proxy, "resource": list(key),
            "status": "serving",
            "previous": list(previous) if previous else None,
            "routes_via": _fmt(closure["resources"]),
        })

    # ------------------------------------------------------------------
    # 连接使用接口
    # ------------------------------------------------------------------

    def connection_opened(self, proxy, listener, conn):
        """连接开始消费当前配置：钉住生效监听器修订的整个闭包。"""
        state = self._proxies.get(str(proxy))
        if state is None:
            return self._finish({"ok": False, "proxy": proxy, "conn": conn,
                                 "status": "unknown-proxy"})
        serving = state.serving.get(str(listener))
        if serving is None:
            return self._finish({
                "ok": False, "proxy": proxy, "listener": listener, "conn": conn,
                "status": "refused-not-serving",
                "note": "监听器尚未激活，无可用配置可消费",
            })
        conn = str(conn)
        if conn in self._connections:
            return self._finish({"ok": False, "conn": conn, "status": "duplicate"})
        closure = self._closure(serving)
        pins = frozenset(closure["resources"] | closure["missing"])
        self._connections[conn] = Connection(state.name, str(listener), pins)
        return self._finish({
            "ok": True, "conn": conn, "proxy": state.name, "listener": listener,
            "status": "pinned",
            "pins": _fmt(pins),
            "note": "切换前建立的连接保留其实际所需的旧引用直至结束",
        })

    def connection_closed(self, conn):
        """连接结束：释放钉住的引用，可能促成延期删除落实。"""
        conn = str(conn)
        existing = self._connections.pop(conn, None)
        if existing is None:
            return self._finish({"ok": False, "conn": conn, "status": "unknown"})
        return self._finish({
            "ok": True, "conn": conn, "status": "released",
            "released_pins": _fmt(existing.pins),
        })

    # ------------------------------------------------------------------
    # 值班员查询接口
    # ------------------------------------------------------------------

    def inspect(self, record):
        """按 record 内容分发到代理或资源查询。"""
        if "proxy" in record:
            return self.inspect_proxy(record["proxy"])
        if "resource" in record:
            pair = record["resource"]
            return self.inspect_resource(pair[0], pair[1])
        if "name" in record and "rev" in record:
            return self.inspect_resource(record["name"], record["rev"])
        return {"ok": False, "error": "inspect 需要 proxy 或 resource 字段"}

    def inspect_proxy(self, proxy):
        """代理全景：各监听器仍在哪路由、候选卡在哪、还缺哪些版本。"""
        state = self._proxies.get(str(proxy))
        if state is None:
            return {"ok": False, "error": "未知代理", "proxy": proxy}
        listeners = {}
        for listener in list(state.listeners) + [
            name for name in state.serving if name not in state.listeners
        ]:
            listeners[listener] = self._candidate_view(state, listener)
        connections = {
            cid: {"listener": conn.listener, "pins": _fmt(conn.pins)}
            for cid, conn in self._connections.items()
            if conn.proxy == state.name
        }
        nonces = {}
        for nonce, batch in state.nonces.items():
            entry = {"status": batch.status, "resources": _fmt(batch.resources)}
            if batch.rejected_resource:
                entry["rejected_resource"] = list(batch.rejected_resource)
            nonces[nonce] = entry
        plan = self.plan_push(state.name)
        return {
            "ok": True,
            "proxy": state.name,
            "epoch": state.epoch,
            "connected": state.connected,
            "have": _fmt(state.have),
            "listeners": listeners,
            "connections": connections,
            "nonces": nonces,
            "plan": {"send": plan["send"], "removes": plan["removes"]},
        }

    def inspect_resource(self, name, rev):
        """资源全景：谁还需要它、删除卡在哪、何时确实可以删除。"""
        key = (str(name), int(rev))
        dependents = self._dependents(key)
        base = {"ok": True, "resource": list(key)}
        if key in self._repo:
            res = self._repo[key]
            deletable = not any(dependents.values())
            return {
                **base,
                "status": "remove-pending" if res.remove_requested else "present",
                "kind": res.kind,
                "refs": _fmt(res.refs),
                "needed_by": dependents,
                "deletable": deletable,
                "note": "现在删除即可落实" if deletable
                        else "仍被需要，删除需等待依赖消失",
            }
        if key in self._hard_removed:
            return {
                **base,
                "status": "force-disabled",
                "stuck": dependents,
                "note": "已被强制停用；stuck 是仍停在该资源上的监听器与连接",
            }
        if key in self._collected:
            return {**base, "status": "collected", "note": "已回收，无人需要"}
        return {**base, "status": "unknown", "note": "从未发布或早已移除"}
