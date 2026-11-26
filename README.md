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

## 读取演练资料

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式及读取接口。

    python3 scenario_reader.py examples
    python3 -m unittest discover -v

资料读取器会按文件统计各类参与方消息的数量。演练记录只描述各参与方交给系统的输入。
