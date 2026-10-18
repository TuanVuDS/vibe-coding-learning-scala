#!/usr/bin/env bash
# Chạy job Chương 26 bằng spark-submit trên Linux/macOS/WSL2 (đã kiểm chứng trên WSL2 Ubuntu 24.04, Spark 4.2.0, Temurin 21.0.12.1).
# Cần: SPARK_HOME trỏ tới spark-4.2.0-bin-hadoop3, JAVA_HOME tới JDK 17/21/25, jar từ `sbt assembly`.
# Chạy từ thư mục spark-production-lab:  OUT=/tmp/ch26 deploy/submit.sh --run-date=2026-10-16 --max-reject-rate=0.2
set -euo pipefail
: "${SPARK_HOME:?đặt SPARK_HOME}"
: "${OUT:?đặt OUT (thư mục output)}"
exec "$SPARK_HOME/bin/spark-submit" \
  --master "${SPARK_MASTER:-local[2]}" \
  --conf spark.ui.enabled=false \
  --class vn.scalaeng.sparkprod.Job \
  target/scala-2.13/spark-production-job.jar \
  --input=../batch-pipeline-lab/data/input \
  --customers=../batch-pipeline-lab/data/customers.csv \
  --output="$OUT" \
  "$@"
