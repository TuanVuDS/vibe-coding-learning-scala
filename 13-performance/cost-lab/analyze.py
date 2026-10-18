"""Tính lại bảng từ mẫu gốc đã lưu; không chạy benchmark hay assert tốc độ."""
import csv
from decimal import Decimal
import math
from pathlib import Path
import re
from collections import defaultdict

OUT = Path(__file__).resolve().parent / "verification"


def percentile(values, p):
    return sorted(values)[math.ceil(len(values) * p) - 1]


def read(name):
    with (OUT / name).open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


def main():
    tables = ["# Bảng tính lại từ raw T20\n",
              "Sinh bằng `python 13-performance/cost-lab/analyze.py`; giá compute **giả định** 0,12 đơn vị tiền/node-hour, 1 node. Cost là mô hình capacity, chưa có startup/idle/sink. CPU/wall lấy tổng CPU chia tổng wall qua 21 lượt; các case rất ngắn chịu độ phân giải CPU thô và thread nền JVM. Discarded-result bỏ kết quả nên rate cơ học không phải capacity nghiệp vụ; không định giá case đó.\n"]
    context = ("Bối cảnh snapshot: i7-9750H, 12 CPU logic, RAM 34.188.873.728 byte, "
               "Windows 11/Adoptium 21.0.12.1, G1, heap 512 MiB. Máy dùng chung; "
               "không có nhật ký job nền để xác nhận tác vụ chạy đồng thời, p99 có thể chịu nhiễu nền.\n")
    cpu_deltas = []
    for mode in ("serialization", "query", "cache", "cpu", "batch-service", "traps"):
        groups = defaultdict(list)
        for row in read(f"{mode}.csv"):
            groups[row["case"]].append(row)
            cpu_deltas.append(int(row["process_cpu_ns"]))
        tables += [f"## {mode}\n", context,
                   "| Case | p50 ms | p95 ms | p99 ms | Event/s từ median | Cost/triệu | CPU/wall | Main alloc median byte | GC count/ms |",
                   "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
        for name, rows in groups.items():
            assert sorted(int(r["round"]) for r in rows) == list(range(21)), name
            assert len({r["events"] for r in rows}) == 1
            assert len({r["checksum"] for r in rows}) == 1
            ns = [int(r["wall_ns"]) for r in rows]
            assert min(ns) > 0
            p50, p95, p99 = [percentile(ns, p) / 1e6 for p in (.5, .95, .99)]
            rate = int(rows[0]["events"]) * 1000 / p50
            cost = Decimal(1000000) / Decimal(str(rate)) / 3600 * Decimal("0.12")
            cost_text = "không hợp lệ" if name == "discarded-result" else f"{cost:.8f}"
            cores = sum(int(r["process_cpu_ns"]) for r in rows) / sum(ns)
            allocated = percentile([int(r["main_thread_allocated_bytes"]) for r in rows], .5)
            gc_count = sum(int(r["gc_count"]) for r in rows)
            gc_ms = sum(int(r["gc_ms"]) for r in rows)
            tables.append(f"| {name} | {p50:.4f} | {p95:.4f} | {p99:.4f} | {rate:.0f} | {cost_text} | {cores:.3f} | {allocated} | {gc_count}/{gc_ms} |")
        tables.append("")

    latencies = defaultdict(list)
    rounds = defaultdict(list)
    for row in read("event-latency.csv"):
        batch, round_id, event = (int(row[k]) for k in ("batch", "round", "event"))
        scheduled, completed, latency = (int(row[k]) for k in ("scheduled_ns", "completed_ns", "latency_ns"))
        assert scheduled == event * 100000
        assert latency == completed - scheduled and latency >= 0
        latencies[batch].append(latency / 1e6)
        rounds[batch, round_id].append((event, latency / 1e6))
    totals = defaultdict(list)
    for row in read("event-totals.csv"):
        assert int(row["events"]) == 2000
        totals[int(row["batch"])].append(int(row["elapsed_ns"]))
    tables += ["## Event latency (scheduled arrival → completion)\n", context,
               "| Cap | Event | p50 ms | p95 ms | p99 ms | Mean ms | Max ms | p99 round min–max ms | Completed/s |",
               "|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for batch, values in sorted(latencies.items()):
        assert len(values) == 18000
        per_round = []
        for round_id in range(9):
            samples = rounds[batch, round_id]
            assert [e for e, _ in samples] == list(range(2000))
            per_round.append(percentile([ms for _, ms in samples], .99))
        assert len(totals[batch]) == 9
        p50, p95, p99 = [percentile(values, p) for p in (.5, .95, .99)]
        rate = len(values) * 1e9 / sum(totals[batch])
        tables.append(f"| {batch} | {len(values)} | {p50:.4f} | {p95:.4f} | {p99:.4f} | {sum(values)/len(values):.4f} | {max(values):.4f} | {min(per_round):.4f}–{max(per_round):.4f} | {rate:.2f} |")

    payload = re.search(r"PAYLOAD events=(\d+) jsonBytes=(\d+) avroBytes=(\d+)", (OUT / "local.txt").read_text(encoding="utf-8"))
    count, json_bytes, avro_bytes = map(int, payload.groups())
    tables += ["\n## Network model (không phải phép đo mạng)\n",
               "Giả định 50.000.000 byte/s effective bandwidth và 0,05 đơn vị tiền/GiB egress; không nén/header/TLS/retry/fan-out. Làm tròn tổng byte lên.\n",
               context,
               "| Codec | Payload byte | Byte/event | Byte/triệu event | Transfer giây/triệu | Egress cost/triệu |",
               "|---|---:|---:|---:|---:|---:|"]
    for codec, size in (("JSONL", json_bytes), ("Avro raw + count", avro_bytes)):
        million = (size * 1000000 + count - 1) // count
        charge = Decimal(million) / (1024 ** 3) * Decimal("0.05")
        tables.append(f"| {codec} | {size} | {size/count:.4f} | {million} | {million/50000000:.6f} | {charge:.8f} |")

    assert min(cpu_deltas) >= 0, "CPU counter unsupported or invalid snapshot"
    positive = [value for value in cpu_deltas if value > 0]
    step = math.gcd(*positive)
    counter = (f"CPU_COUNTER samples={len(cpu_deltas)} zero={cpu_deltas.count(0)} "
               f"minPositiveNs={min(positive)} gcdPositiveNs={step} observedStepMs={step / 1e6}")
    tables += ["\n## Bộ đếm CPU\n", context,
               "API: `com.sun.management.OperatingSystemMXBean.getProcessCpuTime()`. "
               "Tính trên delta trong serialization/query/cache/cpu/batch-service/traps CSV. "
               "Ước chung lớn nhất của delta dương chỉ mô tả bước quan sát của snapshot; "
               "không chứng minh độ phân giải của mọi Windows/JDK.\n",
               f"```text\n{counter}\n```\n"]
    text = "\n".join(tables) + "\n"
    (OUT / "analysis.md").write_text(text, encoding="utf-8")
    print(text)
    print("RAW CHECKS PASS: 21 rounds/case; stable checksums; 9 x 2000 scheduled events/cap, no missing event.")


if __name__ == "__main__":
    main()
