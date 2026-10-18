#!/usr/bin/env bash
# Chạy Project 3 bằng spark-submit trên Linux/macOS/WSL2 (đã kiểm chứng: WSL2 Ubuntu 24.04, Spark 4.2.0, Temurin 21.0.12.1).
# Chạy từ thư mục p3-batch-pipeline sau `sbt assembly`:
#   SPARK_HOME=... IN=data/sample OUT=/tmp/p3 deploy/submit.sh --run-date=2026-10-16 --max-reject-rate=0.3
set -euo pipefail
: "${SPARK_HOME:?đặt SPARK_HOME}"; : "${IN:?đặt IN (thư mục input có orders/, clicks/, customers.csv)}"; : "${OUT:?đặt OUT}"
exec "$SPARK_HOME/bin/spark-submit" \
  --master "${SPARK_MASTER:-local[4]}" \
  --driver-memory "${DRIVER_MEMORY:-2g}" \
  --conf spark.ui.enabled=false \
  --class vn.scalaeng.p3.Job \
  target/scala-2.13/p3-batch-pipeline.jar \
  --orders="$IN/orders" --clicks="$IN/clicks" --customers="$IN/customers.csv" --output="$OUT" "$@"
