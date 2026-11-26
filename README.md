# 服务网格增量配置的依赖闭包与逐代理生效

资源以 name/rev 定位，kind 是 listener、route 或 cluster。refs 是精确 name/rev 列表。nonce 表示给指定 proxy 发的增量；epoch 是代理的连接代次；candidate 是监听器欲采用的确定修订。

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 at、消息名称 kind 与对象 record。at 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 record 中。

- **proxy**　name/epoch 与 listeners，have 为它证明仍持有的资源版本。
- **resource**　name/rev/kind/refs 发布，refs 为 name/rev 列表。
- **send**　proxy/epoch/nonce 给出 resources 与删除项，可用于观察代理收到的增量内容。
- **ack**　proxy/epoch/nonce 的 accepted 或 rejected，后者可给 resource 与原因。
- **activate**　proxy/epoch/name/rev 对候选切换的反馈。
- **opened**　proxy/listener 与 conn 开始消费当前配置。
- **closed**　conn 结束。
- **remove**　申请删除 name/rev；hard 表示外部不再供应。
- **disconnect**　proxy/epoch 断连。
- **restart**　控制面重启。
- **inspect**　查询 proxy 或资源。

## 示例

- out-of-order-nonce-and-rejected-dependency.jsonl
- new-flow-and-old-pin.jsonl
- cyclic-candidate.jsonl
- force-disable-and-rejoin.jsonl

## 读取演练资料

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式及读取接口。

    python3 scenario_reader.py examples
    python3 -m unittest discover -v

资料读取器会按文件统计各类参与方消息的数量。演练记录只描述各参与方交给系统的输入。

## 控制面服务

control_plane.py 实现本地配置分发服务 ControlPlane，把送过哪些资源、代理能不能
激活一套配置、现有连接仍依赖什么解释清楚。接口分四组，所有方法返回 JSON 友好的
dict 说明决策结果。

### 配置推送接口

- **publish(name, rev, kind, refs)**　登记一份资源修订；同 name/rev 重复发布需内容一致。
- **request_remove(name, rev, hard=False)**　申请删除。hard=False 时无人需要才落实，
  否则延期并列出 blockers；hard=True 强制停用确实不可继续供应的资源，stuck 列出仍停在
  该资源上的监听器与连接，不可激活的新候选不能当作替代。
- **plan_push(proxy) / plan_all()**　按订阅与既知资源计算增量：send 是待补资源，
  removes 是代理侧已无人需要、可清理的资源。
- **note_send(proxy, epoch, nonce, resources, removes=())**　登记实际发出的批次，
  供后续确认对账；代次不符、代理断连或 nonce 重复时不予登记。
- **restart()**　控制面重启：在途批次作废，仓库、订阅、激活、连接与删除意向保留。

### 代理报告接口

- **proxy_connected(name, epoch, listeners, have)**　接入或重连。新代次按申报重置
  既知资源，旧代次批次与拒绝记录作废；申报中若含已被强制停用的资源会给出警告。
  响应附带推送计划，新站接进来即可看到它还有哪些版本没到。
- **proxy_disconnected(proxy, epoch)**　断连。在途批次作废，已激活配置与连接保留。
- **proxy_ack(proxy, epoch, nonce, result, resource=None)**　批次确认或拒绝。旧代次、
  未知或已了结的批次一律忽略，不能算代理认可了新候选。拒绝时按被拒绝资源定位受影响
  的监听器（affected_listeners），并给出各监听器原来还能路由到哪（still_routes）；
  彼此无引用的其它监听器继续更新。
- **proxy_activated(proxy, epoch, name, rev)**　候选切换反馈。候选快照无效、引用未
  收齐、候选曾被拒绝或批次未确认时不予接受，代理保持原可用配置——确认收到某份路由
  不等于流量已切换。

### 连接使用接口

- **connection_opened(proxy, listener, conn)**　连接开始消费当前配置，钉住生效监听器
  修订的整个闭包；监听器尚未激活时拒绝。
- **connection_closed(conn)**　连接结束，释放钉住的引用；可能促成延期删除落实
  （响应的 notices 中给出）。

### 值班员查询

- **inspect_proxy(name)**　代理全景：各监听器仍在哪路由（serving / still_routes_via）、
  候选卡在哪一步（status 与 missing / reasons）、还缺哪些版本、连接钉住了什么、
  批次对账情况与当前推送计划。
- **inspect_resource(name, rev)**　资源全景：谁还需要它（needed_by：生效配置、待补
  候选、打开的连接）、deletable 是否现在可删；被强制停用或已回收时给出对应说明。
- **inspect(record)**　按 record 里的 proxy 或 resource 字段分发到上述查询。

候选状态取值：no-published-rev、blocked（缺失引用 / 循环依赖 / 不相容修订）、
awaiting_delivery、sent、rejected、ready、serving。候选快照无效时控制面不会把
该候选列入推送计划；被拒绝的候选保持粘性，直到更高的监听器修订发布或代理进入新代次。

## 运行演练轨迹

run_scenario.py 把演练记录逐条喂给控制面，打印每步决策；inspect 以缩进 JSON 完整
展示，推送计划发生变化时顺带打印。

    python3 run_scenario.py examples/out-of-order-nonce-and-rejected-dependency.jsonl
    python3 run_scenario.py examples/*.jsonl

test_control_plane.py 回放全部演练记录并覆盖边界语义：乱序与旧代次确认不计数、
被拒依赖只影响引用它的监听器、共享资源在一处迁走后仍保留、延期删除跨重启等待
连接结束、循环与不相容修订永不切流、强制停用报告 stuck 且不以不可激活候选充数。
