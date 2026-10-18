"""Regression mã thoát shell bằng Dagster giả lập; không phải kiểm chứng Dagster runtime.

Chạy: python dagster/test-run-dagster.py (cần bash; không cần Python package ngoài).
Giữ thư mục test và log trong .cache/t24; không xóa dữ liệu caller.
"""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class ScriptExitTest(unittest.TestCase):
    def run_case(self, fail_day):
        lab = Path(__file__).resolve().parents[1]
        cache = lab.parents[1] / ".cache" / "t24"
        cache.mkdir(parents=True, exist_ok=True)
        work = Path(tempfile.mkdtemp(prefix="ch31-script-", dir=cache))
        sentinel = work / "caller-sentinel.txt"
        sentinel.write_text("dữ liệu caller", encoding="utf-8")
        stub = work / "dagster-stub.sh"
        stub.write_text('''#!/usr/bin/env bash
set -eu
day="${@: -1}"
if [ "$day" = "${FAIL_DAY:-}" ]; then exit 42; fi
mkdir -p "$CH31_ROOT/_lineage" "$CH31_ROOT/serving/daily_revenue/day=$day"
echo '{}' >> "$CH31_ROOT/_lineage/materializations.jsonl"
echo 'segment,revenue_vnd,order_lines,orders' > "$CH31_ROOT/serving/daily_revenue/day=$day/part.csv"
echo ASSET_MATERIALIZATION
echo ASSET_MATERIALIZATION
''', encoding="utf-8", newline="\n")
        stub.chmod(0o755)
        # WORK tương đối tránh mkdir -p đi lại các thư mục cha ngoài sandbox trên Windows.
        relative_work = Path(os.path.relpath(work, lab)).as_posix()
        env = dict(os.environ, WORK=relative_work, DAGSTER=stub.as_posix(), CH31_JAR="unused-stub.jar", FAIL_DAY=fail_day)
        git_bash = Path(os.environ.get("ProgramFiles", "C:/Program Files")) / "Git/usr/bin/bash.exe"
        bash = os.environ.get("BASH_EXE", str(git_bash) if git_bash.is_file() else "bash")
        result = subprocess.run([bash, "dagster/run-dagster.sh"], cwd=lab, env=env,
                                capture_output=True, encoding="utf-8", timeout=30)
        (work / "stdout.txt").write_text(result.stdout, encoding="utf-8")
        (work / "stderr.txt").write_text(result.stderr, encoding="utf-8")
        self.assertEqual(sentinel.read_text(encoding="utf-8"), "dữ liệu caller")
        self.assertEqual(len(list(work.glob("ch31-dagster.*"))), 1, result.stderr)
        self.assertIn("partition 2026-10-17: exit=0 2 materialization", result.stdout)
        return result

    def test_all_success(self):
        self.assertEqual(self.run_case("").returncode, 0)

    def test_middle_failure_last_success(self):
        result = self.run_case("2026-10-16")
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("partition 2026-10-16: exit=42 0 materialization", result.stdout)


if __name__ == "__main__":
    unittest.main(verbosity=2)
