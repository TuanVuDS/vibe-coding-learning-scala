#!/usr/bin/env bash
# Chạy từng thí nghiệm của Chương 11 bằng Scala CLI, kèm cờ JVM tương ứng.
# Cú pháp: scala-cli run . --main-class <class> --java-opt <cờ JVM>
set -u
run() { echo; echo "===== $1 $2"; scala-cli run . -q --main-class "jvmlab.$1" ${2:+--java-opt "$2"}; }
run jvmInfo "-Xmx512m"
run boxingLab "-Xmx2g"
run opaqueErasure ""
run gcLab "-Xlog:gc"
run oomLab "-Xmx64m"
run stackLab "-Xss256k"
run stackLab "-Xss4m"
run deadlockLab ""
run jfrLab ""
