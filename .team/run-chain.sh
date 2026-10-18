#!/usr/bin/env bash
# Dùng: .team/run-chain.sh <log1> <prompt1> [<log2> <prompt2> ...] — chạy tuần tự nhiều task Codex trong một tiến trình nền.
set -u
cd "$(dirname "$0")/.."
while [ $# -ge 2 ]; do
  .team/run-codex.sh "$1" "$2"
  if tail -3 ".team/logs/$1.log" | grep -q "usage limit"; then echo "Dừng chuỗi: hết hạn mức ở $1"; exit 1; fi
  shift 2
done
