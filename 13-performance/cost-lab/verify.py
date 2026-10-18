"""Chạy từ gốc repo. Các JVM chạy tuần tự; cache và file tạm nằm trong .cache/t20."""
import argparse
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
EVIDENCE = Path(__file__).resolve().parent / "verification"
CACHE = ROOT / ".cache" / "t20"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["test", "demo", "local", "cpu", "batch", "traps", "memory", "all"])
    args = parser.parse_args()
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    (CACHE / "tmp").mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.update(COURSIER_CACHE=str(ROOT / ".cache" / "coursier"), TEMP=str(CACHE / "tmp"), TMP=str(CACHE / "tmp"))

    def run(label, action, target, main_class=None, app_args=(), memory="512m"):
        command = ["scala-cli.bat", action, target, "--server=false", "--suppress-outdated-dependency-warning",
                   "--workspace", str(CACHE / ("build-solutions" if "solutions" in target else "build-lab")),
                   "--java-opt=-Xms128m", f"--java-opt=-Xmx{memory}",
                   f"--java-opt=-Djava.io.tmpdir={CACHE / 'tmp'}", "--java-opt=-Dfile.encoding=UTF-8"]
        if main_class:
            command += ["--main-class", main_class]
        if label == "memory":
            command += ["--java-opt=-Xlog:gc*:file=.cache/t20/memory-gc.log:uptime,level,tags"]
        if app_args:
            command += ["--", *app_args]
        print(f"RUN {label}: {subprocess.list2cmdline(command)}", flush=True)
        result = subprocess.run(command, cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                encoding="utf-8", errors="replace")
        log = "COMMAND " + subprocess.list2cmdline(command) + "\n" + result.stdout + f"\nEXIT {result.returncode}\n"
        (EVIDENCE / f"{label}.txt").write_text(log, encoding="utf-8")
        print(result.stdout, end="", flush=True)
        if result.returncode:
            sys.exit(result.returncode)
        if label == "memory":
            (EVIDENCE / "memory-gc.log").write_bytes((CACHE / "memory-gc.log").read_bytes())

    if args.mode in ("test", "all"):
        run("lab-test", "test", "13-performance/cost-lab")
        run("solutions-test", "test", "solutions/ch34-code")
    if args.mode in ("demo", "all"):
        run("model-demo", "run", "13-performance/cost-lab", "vn.scalaeng.cost.ModelDemo")
    for mode in ("local", "cpu", "batch", "traps"):
        if args.mode in (mode, "all"):
            run(mode, "run", "13-performance/cost-lab", "vn.scalaeng.cost.CostBench", (mode, str(EVIDENCE)))
    if args.mode in ("memory", "all"):
        run("memory", "run", "13-performance/cost-lab", "vn.scalaeng.cost.MemoryProbe", memory="256m")


if __name__ == "__main__":
    main()
