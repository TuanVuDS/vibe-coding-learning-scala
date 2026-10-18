#!/usr/bin/env bash
# Dùng: .team/run-codex.sh <tên-log> <file-prompt>
# stdin PHẢI là /dev/null: `codex exec` đọc thêm input từ stdin nếu stdin mở, và sẽ treo mãi trong shell nền.
set -u
cd "$(dirname "$0")/.."
log=".team/logs/$1.log"
codex exec -s workspace-write -c sandbox_workspace_write.network_access=true -c model_reasoning_effort=high \
  --skip-git-repo-check "$(cat "$2")" < /dev/null > "$log" 2>&1
echo "EXIT=$?" >> "$log"
tail -8 "$log"
