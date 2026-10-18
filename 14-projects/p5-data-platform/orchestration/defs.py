"""Project 5 — ORCHESTRATION bằng Dagster 1.13.26 (Python, công cụ riêng). Mỗi asset gọi MỘT lệnh của job Scala (p5.jar) cho MỘT ngày.

Dagster lo: thứ tự (deps), partition theo ngày, lịch sử chạy, retry cấp run, giao diện backfill.
Scala lo: kiểm hợp đồng khi nhận file, tính staging/daily, quality gate, nạp PostgreSQL.
PostgreSQL lo: lưu bảng phục vụ, view tổng tháng (SQL), transaction khi nạp.

Biến môi trường: P5_LAKE (thư mục lake), P5_DROP (thư mục file thả vào), P5_JAR (p5.jar), P5_JDBC (JDBC URL), P5_JAVA (tùy chọn).
"""
import json
import os
import subprocess

import dagster as dg

LAKE, DROP, JAR, JDBC = os.environ["P5_LAKE"], os.environ["P5_DROP"], os.environ["P5_JAR"], os.environ["P5_JDBC"]
JAVA = os.environ.get("P5_JAVA", "java")

# end_offset=1: cho phép partition của NGÀY ĐANG DIỄN RA (mặc định Dagster chỉ có partition của ngày đã kết thúc — Chương 31 mục 10).
# timezone: mặc định UTC — lúc 00:30 giờ Việt Nam vẫn là "hôm qua" theo UTC (Chương 31 mục 10, lỗi 1).
daily = dg.DailyPartitionsDefinition(start_date="2026-10-15", end_date="2026-10-18", end_offset=1, timezone="Asia/Ho_Chi_Minh")


def p5(context: dg.AssetExecutionContext, *args: str) -> str:
    cmd = [JAVA, "-Dstdout.encoding=UTF-8", "-jar", JAR, *args, f"--root={LAKE}", f"--run-id=dagster-{context.run_id[:8]}"]
    r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8")
    out = r.stdout.strip().splitlines()[-1] if r.stdout.strip() else ""
    if r.returncode != 0:
        # Mã thoát có nghĩa (README mục 3): 3 = file bị cách ly, 4 = quality gate chặn — hiện lên Dagster như một run thất bại có lý do.
        raise dg.Failure(description=f"p5 {args[0]} trả mã {r.returncode}: {out or r.stderr.strip()[-500:]}")
    return out


@dg.asset(partitions_def=daily, group_name="p5")
def raw_orders(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """INGESTION: file thả vào của ngày → kiểm contract → raw (hoặc quarantine)."""
    day = context.partition_key
    out = p5(context, "land-file", f"--file={os.path.join(DROP, f'orders-{day}.jsonl')}", f"--ingest-date={day}")
    return dg.MaterializeResult(metadata={"landed": out})


@dg.asset(partitions_def=daily, deps=[raw_orders], group_name="p5")
def staging_orders(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    m = json.loads(p5(context, "materialize", "--asset=staging_orders", f"--day={context.partition_key}"))
    return dg.MaterializeResult(metadata={k: v for k, v in m["counts"].items()})


@dg.asset(partitions_def=daily, deps=[staging_orders], group_name="p5")
def daily_revenue(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    m = json.loads(p5(context, "materialize", "--asset=daily_revenue", f"--day={context.partition_key}"))
    return dg.MaterializeResult(metadata={"rows": m["counts"]["rows"], "output_hash": m["outputHash"]})


@dg.asset(partitions_def=daily, deps=[daily_revenue], group_name="p5")
def serving_daily_revenue(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Quality gate + nạp PostgreSQL trong một transaction (idempotent)."""
    out = json.loads(p5(context, "publish", f"--day={context.partition_key}", f"--jdbc-url={JDBC}"))
    return dg.MaterializeResult(metadata={"serving": out["serving"]})


@dg.asset(deps=[raw_orders], group_name="p5")
def raw_reconcile(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """ĐỐI SOÁT CẤP RAW (không phân vùng): dòng raw không thuộc ngày nào không bao giờ hiện trong báo cáo theo ngày. Chính sách: không chặn
    publish các ngày hợp lệ, nhưng số lượng và vị trí phải NHÌN THẤY được — metadata ở đây + cảnh báo trong log của run."""
    r = json.loads(p5(context, "reconcile"))
    if r["unassigned"] > 0:
        context.log.warning(f"{r['unassigned']} dòng raw không thuộc ngày nào: {r['unassigned_rows'][:5]}")
    return dg.MaterializeResult(metadata={"raw_rows": r["raw_rows"], "assigned": r["assigned"], "unassigned": r["unassigned"]})


defs = dg.Definitions(assets=[raw_orders, staging_orders, daily_revenue, serving_daily_revenue, raw_reconcile])
