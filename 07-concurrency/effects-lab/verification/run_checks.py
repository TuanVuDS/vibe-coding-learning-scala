"""Chạy từ repo root. Lưu output thật, bỏ ANSI, giữ exit code. Không tự đổi version."""
from datetime import datetime, timezone
from pathlib import Path
import os
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
env = os.environ.copy()
env["COURSIER_CACHE"] = str(HERE.parent / ".cache" / "coursier")
commands = {
    "lab-test": ["scala-cli.bat", "test", "07-concurrency/effects-lab", "--server=false"],
    "solutions-test": ["scala-cli.bat", "test", "solutions/effects-code", "--server=false"],
    "demo": ["scala-cli.bat", "run", "07-concurrency/effects-lab", "--server=false", "--main-class", "vn.scalaeng.effects.EffectsDemo"],
    "simple": ["scala-cli.bat", "run", "07-concurrency/effects-lab", "--server=false", "--main-class", "vn.scalaeng.effects.SimpleExample"],
    "errors": ["scala-cli.bat", "run", "07-concurrency/effects-lab", "--server=false", "--main-class", "vn.scalaeng.effects.ErrorDemo"],
    "sbt-test": ["sbt.bat", "test"],
}
selected = sys.argv[1:] or [name for name in commands if name != "sbt-test"]
for name in selected:
    command = commands[name]
    cwd = ROOT
    task_env = env.copy()
    if name == "sbt-test":
        cwd = HERE.parent
        cache = HERE.parent / ".cache"
        task_env["JAVA_TOOL_OPTIONS"] = " ".join([
            f"-Dsbt.boot.directory={cache / 'sbt-boot'}",
            f"-Dsbt.global.base={cache / 'sbt-global'}",
            f"-Dsbt.ivy.home={cache / 'ivy'}",
            f"-Dsbt.coursier.home={cache / 'coursier'}",
            "-Dsbt.server.autostart=false", "-Dsbt.supershell=false",
        ])
    result = subprocess.run(command, cwd=cwd, env=task_env, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, check=False)
    output = re.sub(r"\x1b\[[0-9;]*m", "", result.stdout.decode("utf-8", errors="replace"))
    header = f"UTC: {datetime.now(timezone.utc).isoformat()}\nCwd: {cwd}\nCommand: {' '.join(command)}\nExit: {result.returncode}\n\n"
    (HERE / (name + ".log")).write_text(header + output, encoding="utf-8")
    print(name, "exit", result.returncode, flush=True)
    if result.returncode:
        print(output.encode("ascii", errors="backslashreplace").decode())
        sys.exit(result.returncode)
