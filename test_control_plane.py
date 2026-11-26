"""控制面行为测试：回放演练记录并覆盖关键语义边界。"""

import json
import unittest
from pathlib import Path

from control_plane import ControlPlane
from run_scenario import dispatch
from scenario_reader import iter_messages

EXAMPLES = Path(__file__).with_name("examples")
SUPPORTED = set(json.loads((EXAMPLES / "message_types.json").read_text(encoding="utf-8")))


def replay(filename):
    """整段回放一份演练记录，返回 (控制面, [(消息, 响应), ...])。"""
    plane = ControlPlane()
    responses = []
    for row in iter_messages(EXAMPLES / filename, SUPPORTED):
        responses.append((row, dispatch(plane, row["kind"], row["record"])))
    return plane, responses


def response_at(responses, at, kind):
    matched = [resp for row, resp in responses
               if row["at"] == at and row["kind"] == kind]
    assert matched, f"at={at} 没有 {kind} 的响应"
    return matched[-1]


class OutOfOrderScenarioTests(unittest.TestCase):
    """乱序确认与被拒依赖：out-of-order-nonce-and-rejected-dependency.jsonl"""

    @classmethod
    def setUpClass(cls):
        cls.plane, cls.responses = replay(
            "out-of-order-nonce-and-rejected-dependency.jsonl")

    def test_reject_marks_only_affected_listener(self):
        resp = response_at(self.responses, 8, "ack")
        self.assertEqual(resp["status"], "rejected")
        self.assertEqual(resp["rejected_resource"], ["fresh", 1])
        self.assertEqual(resp["affected_listeners"], ["entry"])
        # 收到拒绝后原来还能路由到哪
        still = resp["still_routes"]["entry"]
        self.assertEqual(still["serving"], ["entry", 1])
        self.assertEqual(still["routes_via"], [["entry", 1], ["old", 1], ["way", 1]])

    def test_unrelated_listener_keeps_updating(self):
        resp = response_at(self.responses, 10, "activate")
        self.assertTrue(resp["ok"])
        self.assertEqual(resp["status"], "serving")
        self.assertEqual(resp["resource"], ["other", 1])

    def test_rejected_batch_not_counted_as_have(self):
        inspect = response_at(self.responses, 15, "inspect")
        self.assertNotIn(["entry", 2], inspect["have"])
        self.assertNotIn(["fresh", 1], inspect["have"])

    def test_stale_epoch_ack_is_ignored(self):
        resp = response_at(self.responses, 13, "ack")
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["status"], "ignored-stale")
        # 旧批次确认不能算它认可了新候选：重连后仍缺整套新引用
        inspect = response_at(self.responses, 15, "inspect")
        entry = inspect["listeners"]["entry"]
        self.assertEqual(entry["status"], "awaiting_delivery")
        self.assertEqual(entry["missing"],
                         [["entry", 2], ["fresh", 1], ["way", 2]])
        self.assertEqual(entry["serving"], ["entry", 1])

    def test_remove_deferred_by_serving_and_connection(self):
        resp = response_at(self.responses, 14, "remove")
        self.assertEqual(resp["status"], "deferred")
        serving = {(d["proxy"], d["listener"]) for d in resp["blockers"]["serving"]}
        self.assertEqual(serving, {("p", "entry"), ("p", "other")})
        conns = {d["conn"] for d in resp["blockers"]["connections"]}
        self.assertEqual(conns, {"held"})


class NewFlowScenarioTests(unittest.TestCase):
    """新连接用新配置、旧连接钉旧版本：new-flow-and-old-pin.jsonl"""

    @classmethod
    def setUpClass(cls):
        cls.plane, cls.responses = replay("new-flow-and-old-pin.jsonl")

    def test_connections_pin_their_own_config(self):
        old = response_at(self.responses, 5, "opened")
        self.assertEqual(old["pins"], [["c", 1], ["l", 1]])
        new = response_at(self.responses, 10, "opened")
        self.assertEqual(new["pins"], [["c", 2], ["l", 2]])

    def test_remove_deferred_across_restart_until_close(self):
        # restart 之后连接钉住仍然有效
        resp = response_at(self.responses, 12, "remove")
        self.assertEqual(resp["status"], "deferred")
        self.assertEqual([d["conn"] for d in resp["blockers"]["connections"]],
                         ["old"])
        # 旧连接结束，延期删除随即落实
        closed = response_at(self.responses, 13, "closed")
        self.assertIn({"notice": "resource-collected", "resource": ["c", 1],
                       "note": "已无人需要，删除落实"},
                      closed.get("notices", []))
        # 再次申请删除：已回收
        resp = response_at(self.responses, 14, "remove")
        self.assertEqual(resp["status"], "already-collected")

    def test_proxy_side_cleanup_plan(self):
        inspect = response_at(self.responses, 15, "inspect")
        self.assertEqual(inspect["plan"]["send"], [])
        self.assertEqual(inspect["plan"]["removes"], [["c", 1], ["l", 1]])
        self.assertEqual(inspect["listeners"]["l"]["status"], "serving")


class CyclicScenarioTests(unittest.TestCase):
    """循环依赖的候选永远不可切流：cyclic-candidate.jsonl"""

    @classmethod
    def setUpClass(cls):
        cls.plane, cls.responses = replay("cyclic-candidate.jsonl")

    def test_cycle_blocks_candidate(self):
        inspect = response_at(self.responses, 2, "inspect")
        launch = inspect["listeners"]["launch"]
        self.assertEqual(launch["status"], "blocked")
        self.assertEqual(launch["reasons"]["cycles"],
                         [[["launch", 1], ["route", 1], ["launch", 1]]])
        self.assertIsNone(launch["serving"])

    def test_nothing_is_pushed(self):
        self.assertEqual(self.plane.plan_push("p")["send"], [])


class ForceDisableScenarioTests(unittest.TestCase):
    """强制停用、断连重连与恢复：force-disable-and-rejoin.jsonl"""

    @classmethod
    def setUpClass(cls):
        cls.plane, cls.responses = replay("force-disable-and-rejoin.jsonl")

    def test_hard_remove_reports_stuck_listeners_and_connections(self):
        resp = response_at(self.responses, 9, "remove")
        self.assertEqual(resp["status"], "force-disabled")
        self.assertEqual(resp["stuck"]["serving"],
                         [{"proxy": "q", "listener": "l", "rev": 1}])
        self.assertEqual([d["conn"] for d in resp["stuck"]["connections"]],
                         ["sticky"])

    def test_stuck_report_after_removal(self):
        inspect = response_at(self.responses, 10, "inspect")
        self.assertEqual(inspect["status"], "force-disabled")
        self.assertEqual([d["conn"] for d in inspect["stuck"]["connections"]],
                         ["sticky"])

    def test_inactivatable_candidate_is_not_a_replacement(self):
        inspect = response_at(self.responses, 11, "inspect")
        listener = inspect["listeners"]["l"]
        # 生效配置降级但仍停在旧资源上
        self.assertEqual(listener["serving"], ["l", 1])
        self.assertEqual(listener["serving_degraded"]["missing"], [["c", 1]])
        # 候选被拒绝，不能当成替代
        self.assertEqual(listener["status"], "rejected")
        self.assertEqual(listener["rejected_resource"], ["c", 2])

    def test_rejoin_declares_held_versions_with_warning(self):
        resp = response_at(self.responses, 14, "proxy")
        self.assertEqual(resp["status"], "connected")
        warnings = resp.get("warnings", [])
        self.assertEqual(warnings[0]["warning"], "declared-force-disabled")
        self.assertEqual(warnings[0]["resources"], [["c", 1]])

    def test_recovery_and_cleanup(self):
        activated = response_at(self.responses, 18, "activate")
        self.assertEqual(activated["status"], "serving")
        deletes = [resp for row, resp in self.responses
                   if row["kind"] == "remove" and not row["record"]["hard"]
                   and row["at"] == 19]
        self.assertEqual([d["status"] for d in deletes], ["deleted", "deleted"])
        inspect = response_at(self.responses, 21, "inspect")
        self.assertEqual(inspect["status"], "collected")
        proxy = response_at(self.responses, 20, "inspect")
        self.assertEqual(proxy["listeners"]["l"]["serving"], ["l", 3])
        self.assertEqual(proxy["plan"]["removes"], [["c", 1], ["l", 1], ["r", 1]])


class CandidateValidityTests(unittest.TestCase):
    """候选快照校验与逐代理生效的边界。"""

    def test_incompatible_revisions_block_candidate(self):
        plane = ControlPlane()
        plane.publish("y", 1, "cluster", [])
        plane.publish("y", 2, "cluster", [])
        plane.publish("x", 1, "listener", [["y", 1], ["y", 2]])
        plane.proxy_connected("p", 1, ["x"], [])
        view = plane.inspect_proxy("p")["listeners"]["x"]
        self.assertEqual(view["status"], "blocked")
        self.assertEqual(view["reasons"]["incompatible_revs"], {"y": [1, 2]})
        self.assertEqual(plane.plan_push("p")["send"], [])

    def test_missing_refs_block_then_backfill(self):
        plane = ControlPlane()
        plane.publish("l", 1, "listener", [["r", 1]])
        plane.proxy_connected("p", 1, ["l"], [])
        view = plane.inspect_proxy("p")["listeners"]["l"]
        self.assertEqual(view["status"], "blocked")
        self.assertEqual(view["reasons"]["missing"], [["r", 1]])
        self.assertEqual(plane.plan_push("p")["send"], [])
        # 依赖到齐后自动转为待补下发
        plane.publish("r", 1, "route", [])
        view = plane.inspect_proxy("p")["listeners"]["l"]
        self.assertEqual(view["status"], "awaiting_delivery")
        self.assertEqual(plane.plan_push("p")["send"], [["l", 1], ["r", 1]])

    def test_activate_requires_full_closure(self):
        # 事故复盘：确认收到路由 ≠ 流量已切换，路由还引用着代理没有的集群
        plane = ControlPlane()
        plane.publish("c", 1, "cluster", [])
        plane.publish("r", 1, "route", [["c", 1]])
        plane.publish("l", 1, "listener", [["r", 1]])
        plane.proxy_connected("p", 1, ["l"], [])
        plane.note_send("p", 1, "n1", [["l", 1], ["r", 1]])
        plane.proxy_ack("p", 1, "n1", "accepted")
        resp = plane.proxy_activated("p", 1, "l", 1)
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["status"], "refused-missing-refs")
        self.assertEqual(resp["missing"], [["c", 1]])
        plane.note_send("p", 1, "n2", [["c", 1]])
        plane.proxy_ack("p", 1, "n2", "accepted")
        resp = plane.proxy_activated("p", 1, "l", 1)
        self.assertTrue(resp["ok"])
        self.assertEqual(resp["status"], "serving")

    def test_shared_route_survives_first_listener_migrating(self):
        plane = ControlPlane()
        plane.publish("c1", 1, "cluster", [])
        plane.publish("c2", 1, "cluster", [])
        plane.publish("w", 1, "route", [["c1", 1]])
        plane.publish("w", 2, "route", [["c2", 1]])
        plane.publish("a", 1, "listener", [["w", 1]])
        plane.publish("b", 1, "listener", [["w", 1]])
        plane.publish("a", 2, "listener", [["w", 2]])
        plane.publish("b", 2, "listener", [["w", 2]])
        plane.proxy_connected("p", 1, ["a", "b"], [])
        plane.note_send("p", 1, "n1", [["a", 1], ["b", 1], ["w", 1], ["c1", 1]])
        plane.proxy_ack("p", 1, "n1", "accepted")
        plane.proxy_activated("p", 1, "a", 1)
        plane.proxy_activated("p", 1, "b", 1)
        plane.note_send("p", 1, "n2", [["a", 2], ["b", 2], ["w", 2], ["c2", 1]])
        plane.proxy_ack("p", 1, "n2", "accepted")
        plane.proxy_activated("p", 1, "a", 2)
        # a 已迁走，但 b 仍引用 w/1，不能删
        resp = plane.request_remove("w", 1)
        self.assertEqual(resp["status"], "deferred")
        self.assertEqual([d["listener"] for d in resp["blockers"]["serving"]],
                         ["b"])
        # b 也迁走后，延期删除落实
        resp = plane.proxy_activated("p", 1, "b", 2)
        self.assertIn(["w", 1],
                      [n["resource"] for n in resp.get("notices", [])])

    def test_send_registration_guards(self):
        plane = ControlPlane()
        plane.publish("l", 1, "listener", [])
        plane.proxy_connected("p", 1, ["l"], [])
        self.assertEqual(plane.note_send("p", 2, "n", [])["status"], "stale")
        self.assertTrue(plane.note_send("p", 1, "n", [["l", 1]])["ok"])
        self.assertEqual(plane.note_send("p", 1, "n", [["l", 1]])["status"],
                         "duplicate")
        # 已了结的批次再确认也不生效
        plane.proxy_ack("p", 1, "n", "accepted")
        resp = plane.proxy_ack("p", 1, "n", "accepted")
        self.assertEqual(resp["status"], "ignored-already-resolved")

    def test_inspect_resource_deletable(self):
        plane = ControlPlane()
        plane.publish("c", 1, "cluster", [])
        view = plane.inspect_resource("c", 1)
        self.assertEqual(view["status"], "present")
        self.assertTrue(view["deletable"])
        plane.publish("l", 1, "listener", [["c", 1]])
        plane.proxy_connected("p", 1, ["l"], [])
        view = plane.inspect_resource("c", 1)
        self.assertFalse(view["deletable"])
        self.assertEqual(view["needed_by"]["candidates"],
                         [{"proxy": "p", "listener": "l", "rev": 1}])


if __name__ == "__main__":
    unittest.main()
