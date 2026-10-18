"""Tạo bản sao trong .cache/, biên dịch bản đúng trước khi cố ý gây lỗi.

Chạy từ repo root: python 07-concurrency/effects-lab/verification/compile_errors.py
Không đưa source lỗi vào src/ hoặc thay đổi cờ compiler của lab.
"""
from datetime import datetime, timezone
from pathlib import Path
import os
import re
import subprocess

HERE = Path(__file__).resolve().parent
LAB = HERE.parent
WORK = LAB / ".cache" / "compiler-examples"
env = os.environ.copy()
env["COURSIER_CACHE"] = str(LAB / ".cache" / "coursier")

CASES = {
    "missing-syntax": (
        """package vn.scalaeng.effects
import cats.effect.IO
import cats.syntax.all.*
object MissingSyntax:
  val quotes: IO[List[Long]] = List(120000L, 20000L).parTraverse(n => IO.pure(n))
""", "import cats.syntax.all.*\n", ""),
    "missing-runtime": (
        """package vn.scalaeng.effects
import cats.effect.IO
import cats.effect.unsafe.implicits.global
object MissingRuntime:
  def run(): Unit = IO.println("order accepted").unsafeRunSync()
""", "import cats.effect.unsafe.implicits.global\n", ""),
    "discarded-io": (
        """package vn.scalaeng.effects
import cats.effect.IO
object DiscardedIO:
  def action: IO[Unit] = IO.println("order accepted")
  def build(): IO[Unit] =
    val ignored = action
    IO.unit
""", "    val ignored = action\n", "    action\n"),
}


def compile_case(name, directory, options, expected):
    # Chỉ định file tường minh: Scala CLI bỏ qua .cache khi quét thư mục.
    command = ["scala-cli.bat", "compile", "project.scala", "Example.scala", "--server=false", *options]
    result = subprocess.run(command, cwd=directory, env=env, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, check=False)
    output = re.sub(r"\x1b\[[0-9;]*m", "", result.stdout.decode("utf-8", errors="replace"))
    header = (f"UTC: {datetime.now(timezone.utc).isoformat()}\nCwd: {directory}\n"
              f"Command: {' '.join(command)}\nExit: {result.returncode}\n\n")
    (HERE / f"compiler-{name}.log").write_text(header + output, encoding="utf-8")
    print(f"{name}: exit {result.returncode}", flush=True)
    assert (result.returncode == 0) == expected, output
    return output


for name, (source, old, new) in CASES.items():
    directory = WORK / name
    directory.mkdir(parents=True, exist_ok=True)
    # Giữ đúng Scala/JDK/dependency/cờ của project.scala gốc. Với ví dụ discard,
    # bản đúng ghép action vào IO trả về thay vì bỏ một giá trị IO.
    (directory / "project.scala").write_bytes((LAB / "project.scala").read_bytes())
    if name == "discarded-io":
        source = source.replace("    val ignored = action\n    IO.unit", "    action *> IO.unit")
        old, new = "    action *> IO.unit", "    action\n    IO.unit"
    path = directory / "Example.scala"
    path.write_text(source, encoding="utf-8")
    compile_case(name + "-baseline", directory, [], True)
    path.write_text(source.replace(old, new), encoding="utf-8")
    if name == "discarded-io":
        compile_case(name + "-value-discard", directory, ["--scalac-option", "-Wvalue-discard"], True)
        output = compile_case(name + "-nonunit-statement", directory,
                              ["--scalac-option", "-Wnonunit-statement"], False)
        assert "unused value of type" in output, output
    else:
        compile_case(name, directory, [], False)
