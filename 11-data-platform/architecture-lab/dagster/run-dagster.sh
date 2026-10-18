#!/usr/bin/env bash
# Kiểm chứng ví dụ Dagster của Chương 31 (đã chạy trên Windows 11 + Git Bash, Python 3.12.10, Dagster 1.13.26).
# Chạy từ thư mục architecture-lab, sau khi:
#   python -m venv <venv> && <venv>/Scripts/python -m pip install dagster==1.13.26
#   scala-cli --power package . --assembly --main-class vn.scalaeng.platform.Main -o <jar>
# Dùng: DAGSTER=<venv>/Scripts/dagster CH31_JAR=<jar> WORK=<thư mục đã có> dagster/run-dagster.sh
# Script KHÔNG xóa gì: mỗi lần chạy tạo một thư mục con MỚI trong WORK (mktemp -d) và giữ lại toàn bộ log để xem. Dọn dẹp là việc của bạn.
set -euo pipefail
: "${DAGSTER:?đặt DAGSTER}"; : "${CH31_JAR:?đặt CH31_JAR}"; : "${WORK:?đặt WORK}"
[ -d "$WORK" ] || { echo "WORK=$WORK không phải thư mục đã tồn tại" >&2; exit 2; }
RUN=$(mktemp -d "$WORK/ch31-dagster.XXXXXX")
mkdir -p "$RUN/home" "$RUN/platform"
cp -r data/. "$RUN/platform/"
export DAGSTER_HOME="$RUN/home" CH31_ROOT="$RUN/platform" CH31_JAR
echo "thư mục lần chạy: $RUN"
failed=0
for day in 2026-10-15 2026-10-16 2026-10-17; do
  code=0
  "$DAGSTER" asset materialize -f dagster/defs.py --select "staging_orders,daily_revenue" --partition "$day" > "$RUN/run-$day.log" 2>&1 || code=$?
  count=$(grep -c 'ASSET_MATERIALIZATION' "$RUN/run-$day.log" || true)
  echo "partition $day: exit=$code $count materialization"
  if [ "$code" -ne 0 ]; then failed=1; fi
done
echo "== lineage do job Scala ghi:"
cut -c1-160 "$RUN/platform/_lineage/materializations.jsonl" || failed=1
echo "== serving/daily_revenue/day=2026-10-17:"
cat "$RUN/platform/serving/daily_revenue/day=2026-10-17/part.csv" || failed=1
echo "log giữ tại: $RUN"
exit "$failed"
