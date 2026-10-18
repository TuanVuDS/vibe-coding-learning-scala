"""Ví dụ ORCHESTRATION cho Chương 31 — Dagster 1.13.26 (Python) là công cụ RIÊNG, không phải một phần của Scala.

Dagster quyết định CHẠY GÌ, KHI NÀO, THEO THỨ TỰ NÀO và GHI LẠI kết quả (lịch, backfill, retry cấp run, giao diện).
Job Scala (`architecture-lab`, CLI `materialize`) quyết định TÍNH NHƯ THẾ NÀO cho một partition. Hai vai trò tách biệt:
thay Dagster bằng Airflow/cron không phải sửa logic Scala; sửa logic Scala không phải sửa lịch.

Biến môi trường:
  CH31_ROOT  thư mục platform (raw/, staging/, serving/, _lineage/)
  CH31_JAR   jar do `scala-cli --power package . --assembly` tạo
  CH31_JAVA  (tùy chọn) đường dẫn java, mặc định "java"
"""
import json
import os
import subprocess

import dagster as dg

ROOT = os.environ["CH31_ROOT"]
JAR = os.environ["CH31_JAR"]
JAVA = os.environ.get("CH31_JAVA", "java")

# end_date KHÔNG bao gồm: các partition là 2026-10-15, 2026-10-16, 2026-10-17.
# Mặc định (end_offset=0) Dagster chỉ tạo partition cho ngày ĐÃ KẾT THÚC: chạy trong ngày 17/10 thì key "2026-10-17" chưa tồn tại
# (DagsterUnknownPartitionError — gặp thật khi viết bài). end_offset=1 cho phép partition của ngày đang diễn ra — một QUYẾT ĐỊNH nghiệp vụ:
# số liệu hôm nay chưa đầy đủ và sẽ được tính lại khi lô muộn đến.
# timezone: mặc định Dagster cắt ngày theo UTC — lúc 00:30 giờ Việt Nam ngày 17 vẫn là ngày 16 UTC nên key "2026-10-17" CHƯA có.
# Lab coi "hôm nay" theo giờ Việt Nam nên khai báo tường minh (đổi múi giờ là quyết định nghiệp vụ, không phải chi tiết kỹ thuật).
daily = dg.DailyPartitionsDefinition(start_date="2026-10-15", end_date="2026-10-18", end_offset=1, timezone="Asia/Ho_Chi_Minh")


def run_scala(context: dg.AssetExecutionContext, asset: str) -> dg.MaterializeResult:
    """Gọi job Scala cho ĐÚNG một partition; mã thoát khác 0 là run thất bại (Dagster hiển thị và cho retry)."""
    cmd = [JAVA, "-Dstdout.encoding=UTF-8", "-jar", JAR, "materialize", f"--root={ROOT}", f"--asset={asset}",
           f"--day={context.partition_key}", f"--run-id=dagster-{context.run_id[:8]}"]
    result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8")
    if result.returncode != 0:
        raise dg.Failure(description=f"job Scala trả mã {result.returncode}: {(result.stderr or result.stdout).strip()}")
    lineage = json.loads(result.stdout.strip().splitlines()[-1])  # dòng lineage do job Scala in ra
    return dg.MaterializeResult(metadata={
        "rows": lineage["rows"], "rejected": lineage["rejected"],
        "output_hash": lineage["outputHash"], "scala_code_version": lineage["codeVersion"],
    })


@dg.asset(partitions_def=daily, code_version="v1", group_name="orders")
def staging_orders(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return run_scala(context, "staging_orders")


@dg.asset(partitions_def=daily, deps=[staging_orders], code_version="v1", group_name="orders")
def daily_revenue(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    return run_scala(context, "daily_revenue")


defs = dg.Definitions(assets=[staging_orders, daily_revenue])
