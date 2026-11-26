"""把演练记录逐条喂给控制面，打印每一步的决策轨迹。

用法：

    python3 run_scenario.py examples/out-of-order-nonce-and-rejected-dependency.jsonl
    python3 run_scenario.py examples/*.jsonl

每条输入消息打印一行控制面的响应；inspect 以缩进 JSON 完整展示。
消息处理完后若某代理的推送计划发生变化（待补资源或可清理资源），
顺带打印最新计划，便于观察“只补充实际需要的改动”。
"""

import argparse
import json
from pathlib import Path

from control_plane import ControlPlane
from scenario_reader import iter_messages

DEFAULT_KINDS = {
    "ack", "activate", "closed", "disconnect", "inspect", "opened",
    "proxy", "remove", "resource", "restart", "send",
}


def dispatch(plane, kind, record):
    """把一条演练消息分发给控制面对应的接口。"""
    if kind == "proxy":
        return plane.proxy_connected(record["name"], record["epoch"],
                                     record.get("listeners", []),
                                     record.get("have", []))
    if kind == "resource":
        return plane.publish(record["name"], record["rev"], record["kind"],
                             record.get("refs", []))
    if kind == "send":
        return plane.note_send(record["proxy"], record["epoch"], record["nonce"],
                               record.get("resources", []),
                               record.get("removes", []))
    if kind == "ack":
        return plane.proxy_ack(record["proxy"], record["epoch"], record["nonce"],
                               record["result"], record.get("resource"),
                               record.get("reason"))
    if kind == "activate":
        return plane.proxy_activated(record["proxy"], record["epoch"],
                                     record["name"], record["rev"])
    if kind == "opened":
        return plane.connection_opened(record["proxy"], record["listener"],
                                       record["conn"])
    if kind == "closed":
        return plane.connection_closed(record["conn"])
    if kind == "remove":
        return plane.request_remove(record["name"], record["rev"],
                                    record.get("hard", False))
    if kind == "disconnect":
        return plane.proxy_disconnected(record["proxy"], record["epoch"])
    if kind == "restart":
        return plane.restart()
    if kind == "inspect":
        return plane.inspect(record)
    raise ValueError(f"未登记的消息名称：{kind}")


def pretty(value, level=0):
    """缩进打印 JSON，但 [name, rev] 这类纯标量列表保持单行。"""
    pad = "  " * level
    if isinstance(value, dict):
        if not value:
            return "{}"
        items = [
            f"{'  ' * (level + 1)}{json.dumps(k, ensure_ascii=False)}: "
            f"{pretty(v, level + 1)}"
            for k, v in value.items()
        ]
        return "{\n" + ",\n".join(items) + "\n" + pad + "}"
    if isinstance(value, list):
        if not value:
            return "[]"
        if all(not isinstance(item, (dict, list)) for item in value):
            return json.dumps(value, ensure_ascii=False)
        items = [f"{'  ' * (level + 1)}{pretty(item, level + 1)}"
                 for item in value]
        return "[\n" + ",\n".join(items) + "\n" + pad + "]"
    return json.dumps(value, ensure_ascii=False)


def run(path, supported):
    plane = ControlPlane()
    plans = {}
    print(f"===== {path.name} =====")
    for row in iter_messages(path, supported):
        response = dispatch(plane, row["kind"], row["record"])
        if row["kind"] == "inspect":
            body = "\n" + pretty(response)
        else:
            body = json.dumps(response, ensure_ascii=False)
        print(f"at={row['at']:<4} {row['kind']:<10} {body}")
        for name, plan in plane.plan_all().items():
            summary = {"send": plan["send"], "removes": plan["removes"]}
            previous = plans.get(name)
            changed = summary != previous
            interesting = (summary["send"] or summary["removes"]
                           or (previous and (previous["send"] or previous["removes"])))
            if changed and interesting:
                print(f"     plan[{name}] → {json.dumps(summary, ensure_ascii=False)}")
            plans[name] = summary
    print()


def main():
    parser = argparse.ArgumentParser(description="把演练记录逐条喂给控制面并打印决策轨迹")
    parser.add_argument("scenarios", nargs="+", type=Path, help="JSONL 演练记录")
    args = parser.parse_args()
    for path in args.scenarios:
        registry = path.parent / "message_types.json"
        supported = (set(json.loads(registry.read_text(encoding="utf-8")))
                     if registry.exists() else set(DEFAULT_KINDS))
        run(path, supported)


if __name__ == "__main__":
    main()
