#!/usr/bin/env bash
# Demo toàn luồng P5 (bản trước đã chạy trên Windows 11 + Git Bash; cơ chế stop-file Vòng 3 chưa kiểm chứng runtime trong sandbox):
#   PostgreSQL (nhúng, công cụ dev) ← Dagster → p5.jar: land-file → materialize staging/daily → publish → reconcile → API.
# Chạy từ thư mục p5-data-platform sau `sbt app/assembly`:
#   DAGSTER=<venv>/Scripts/dagster WORK=<thư mục đã có> orchestration/run-platform.sh
# Script KHÔNG xóa gì của bạn: mỗi lần chạy tạo một thư mục con MỚI trong WORK (mktemp -d) và giữ lại mọi log.
# Mã thoát: 0 khi MỌI bước thành công; 1 nếu có bước nào hỏng (Dagster, API) — CI dựa vào được (review P5 M6).
set -uo pipefail
: "${DAGSTER:?đặt DAGSTER}"; : "${WORK:?đặt WORK}"
[ -d "$WORK" ] || { echo "WORK=$WORK không phải thư mục đã tồn tại" >&2; exit 1; }
JAR="$(pwd)/app/target/scala-3.9.0/p5.jar"; PORT=55432; API=8085
SCALA_CLI="${SCALA_CLI:-scala-cli}"   # trên Windows/Git Bash: SCALA_CLI=scala-cli.bat
[ -f "$JAR" ] || { echo "thiếu $JAR — chạy sbt app/assembly trước" >&2; exit 1; }
RUN=$(mktemp -d "$WORK/p5-run.XXXXXX") || exit 1
mkdir -p "$RUN/home" "$RUN/lake/dimension" || exit 1
cp data/dimension/customers.csv "$RUN/lake/dimension/" || exit 1
printf '%s\n' p5-run-v1 > "$RUN/p5-run.owner" || exit 1
# Java/Python Windows cần đường dẫn native kể cả khi shell dùng WORK tương đối hoặc /c/... .
native_path() { if command -v cygpath >/dev/null 2>&1; then cygpath -am "$1"; else (cd "$1" && pwd -P); fi; }
RUN_NATIVE=$(native_path "$RUN") || exit 1
echo "thư mục lần chạy: $RUN"

FAILED=0; PG=""; APIPID=""
fail() { echo "HỎNG: $*"; FAILED=1; }
port_busy() { timeout --kill-after=1s 2s bash -c 'exec 4<>"/dev/tcp/127.0.0.1/$1"' _ "$PORT" 2>/dev/null; }
# Chỉ wait sau khi tiến trình đã thoát; không có wait vô hạn (kể cả API/scala-cli.bat).
wait_until_exit() {
  local process=$1 deadline=$((SECONDS + $2))
  while kill -0 "$process" 2>/dev/null; do
    [ "$SECONDS" -lt "$deadline" ] || return 1
    sleep 0.2
  done
  return 0
}
stop_api() {
  [ -n "$APIPID" ] || return 0
  kill "$APIPID" 2>/dev/null || true
  if ! wait_until_exit "$APIPID" 5; then
    kill -KILL "$APIPID" 2>/dev/null || true
    wait_until_exit "$APIPID" 3 || { fail "API không dừng trong hạn"; return 1; }
  fi
  wait "$APIPID" 2>/dev/null || true  # signal khi dừng API theo kế hoạch không phải lỗi job.
  APIPID=""
}
# Chỉ diệt tiến trình có PID + start-time + commandline thuộc run này. PostgreSQL có thể đã tách khỏi cây launcher.
stop_owned_process() {
  local kind=$1 process
  [ -f "$RUN/$kind.pid" ] || { [ "$kind" = pg ] && [ -f "$RUN/pg-data/postmaster.pid" ]; } || return 0
  if command -v powershell.exe >/dev/null 2>&1; then
    P5_STOP_KIND="$kind" P5_STOP_RUN="$RUN_NATIVE" timeout --kill-after=2s 12s powershell.exe -NoProfile -NonInteractive -Command - <<'POWERSHELL'
$ErrorActionPreference = 'Stop'
$runPath = $env:P5_STOP_RUN
$kind = $env:P5_STOP_KIND
if (Test-Path -LiteralPath (Join-Path $runPath "$kind.pid")) {
  $processId = [int](Get-Content -LiteralPath (Join-Path $runPath "$kind.pid"))
  $started = [long](Get-Content -LiteralPath (Join-Path $runPath "$kind.started"))
} elseif ($kind -eq 'pg') {
  # Start có thể hỏng trước khi ghi pg.pid: dùng PID/start-time trong cluster riêng, vẫn phải kiểm commandline.
  $postmaster = Get-Content -LiteralPath (Join-Path $runPath 'pg-data/postmaster.pid')
  $processId = [int]$postmaster[0]
  $started = [long]$postmaster[2] * 1000
} else { exit 0 }
$ownedProcess = Get-Process -Id $processId -ErrorAction SilentlyContinue
if ($null -eq $ownedProcess) { exit 0 }
$actual = ([DateTimeOffset]$ownedProcess.StartTime.ToUniversalTime()).ToUnixTimeMilliseconds()
if ([Math]::Abs($actual - $started) -ge 1000) { throw 'PID đã được tái sử dụng; từ chối kill' }
$processInfo = Get-CimInstance Win32_Process -Filter "ProcessId = $processId"
$expected = $runPath.Replace('\', '/')
if ($kind -eq 'pg') { $expected += '/pg-data' }
if ($null -eq $processInfo.CommandLine -or $processInfo.CommandLine.Replace('\', '/').IndexOf($expected, [StringComparison]::OrdinalIgnoreCase) -lt 0) {
  throw 'Tiến trình không thuộc run-dir; từ chối kill'
}
if ($kind -eq 'pg' -and $ownedProcess.ProcessName -ne 'postgres') { throw 'PID không phải PostgreSQL; từ chối kill' }
& taskkill.exe /PID $processId /T /F
if ($LASTEXITCODE -ne 0) { exit 1 }
POWERSHELL
  else
    if [ -f "$RUN/$kind.pid" ]; then process=$(tr -d '\r\n' < "$RUN/$kind.pid")
    else process=$(head -n 1 "$RUN/pg-data/postmaster.pid"); fi
    [[ "$process" =~ ^[0-9]+$ ]] || return 1
    ps -p "$process" -o args= | grep -F -- "$RUN_NATIVE" >/dev/null || return 0
    kill -TERM "$process" 2>/dev/null || true
    sleep 1
    ps -p "$process" -o args= | grep -F -- "$RUN_NATIVE" >/dev/null && kill -KILL "$process" 2>/dev/null || true
  fi
}
# Dọn tiến trình do CHÍNH script tạo, rồi thoát bằng đúng mã của script (trap không được nuốt mã lỗi).
cleanup() {
  local rc=$?
  trap - EXIT INT TERM HUP
  [ "$rc" -eq 0 ] || FAILED=1
  stop_api || true
  if [ -n "$PG" ]; then
    # File stop tồn tại cả khi launcher còn compile; JVM khởi động trễ phải kiểm file này trước start PostgreSQL.
    touch "$RUN/stop" || fail "không ghi được stop-file"
    if ! wait_until_exit "$PG" 30; then fail "PostgreSQL/launcher không dừng sau 30 giây"; fi
    if [ ! -f "$RUN/pg.stopped" ] || port_busy; then
      fail "thiếu xác nhận PostgreSQL đã dừng; chạy pg_ctl dự phòng (xem postgres-stop.log)"
      local ctl
      for ctl in "$RUN"/pg-binaries/PG-*/bin/pg_ctl{.exe,}; do
        [ -f "$ctl" ] || continue
        # pg_ctl chọn đúng cluster bằng -D, không theo tên postgres.exe hay chỉ theo cổng.
        timeout --kill-after=2s 15s "$ctl" -D "$RUN_NATIVE/pg-data" -m immediate -t 10 -w stop >> "$RUN/postgres-stop.log" 2>&1 || true
        break
      done
    fi
    if port_busy; then
      stop_owned_process pg >> "$RUN/postgres-stop.log" 2>&1 || fail "không dừng được PostgreSQL của lần chạy"
    fi
    if kill -0 "$PG" 2>/dev/null || port_busy; then
      stop_owned_process jvm >> "$RUN/postgres-stop.log" 2>&1 || fail "không dừng được JVM của lần chạy"
      kill "$PG" 2>/dev/null || true
      wait_until_exit "$PG" 5 || { kill -KILL "$PG" 2>/dev/null || true; }
    fi
    if wait_until_exit "$PG" 3; then
      wait "$PG" 2>/dev/null || fail "LocalPostgres/launcher trả lỗi"
    else fail "launcher chưa thoát; giữ log và stop-file"; fi
    local deadline=$((SECONDS + 5))
    while port_busy && [ "$SECONDS" -lt "$deadline" ]; do sleep 0.2; done
    port_busy && fail "cổng $PORT vẫn bận sau cleanup; xem $RUN/postgres-stop.log"
  fi
  echo "== CLEANUP: exit=$FAILED; giữ dữ liệu/log tại $RUN"
  exit "$FAILED"
}
trap cleanup EXIT
trap 'exit 1' INT TERM HUP

# Cổng đã có người nghe (thường là PostgreSQL của lần chạy trước chưa dừng) → dừng ngay, đừng âm thầm dùng database cũ.
if port_busy; then echo "cổng $PORT đang bận — dừng tiến trình đang giữ nó trước" >&2; exit 1; fi
"$SCALA_CLI" run orchestration/LocalPostgres.scala --server=false -- $PORT "$RUN_NATIVE" </dev/null > "$RUN/postgres.log" 2>&1 &
PG=$!
for _ in $(seq 1 120); do
  [ -f "$RUN/pg.ready" ] && break
  kill -0 "$PG" 2>/dev/null || break
  sleep 2
done
[ -f "$RUN/pg.ready" ] && port_busy || { echo "PostgreSQL không khởi động:"; tail -5 "$RUN/postgres.log"; exit 1; }
JDBC="jdbc:postgresql://localhost:$PORT/postgres?user=postgres"
echo "PostgreSQL: $(grep READY "$RUN/postgres.log")"

export DAGSTER_HOME="$RUN_NATIVE/home" P5_LAKE="$RUN_NATIVE/lake" P5_DROP="$(native_path data/drop)" P5_JAR="$JAR" P5_JDBC="$JDBC"
materialize() { # $1 = nhãn, $2 = danh sách asset, $3.. = tham số thêm
  local label=$1 select=$2; shift 2
  local code=0
  "$DAGSTER" asset materialize -f orchestration/defs.py --select "$select" "$@" > "$RUN/$label.log" 2>&1 || code=$?
  echo "$label: exit=$code $(grep -c ASSET_MATERIALIZATION "$RUN/$label.log") asset được tính"
  [ "$code" -eq 0 ] || fail "$label (xem $RUN/$label.log)"
}
api_up() {
  java -jar "$JAR" serve --jdbc-url="$JDBC" --port=$API > "$RUN/api.log" 2>&1 &
  APIPID=$!
  for _ in $(seq 1 60); do curl --connect-timeout 2 --max-time 5 -fsS "http://127.0.0.1:$API/health/ready" > /dev/null 2>&1 && return 0; sleep 1; done
  fail "API không sẵn sàng (xem $RUN/api.log)"; return 1
}
api_down() { stop_api; }
get() { echo "== GET $1"; curl --connect-timeout 2 --max-time 15 -fsS "http://127.0.0.1:$API$1" || fail "GET $1"; echo; }

for day in 2026-10-15 2026-10-16 2026-10-17; do
  materialize "partition-$day" "raw_orders,staging_orders,daily_revenue,serving_daily_revenue" --partition "$day"
done
materialize "raw-reconcile" "raw_reconcile"
echo "== _quality/raw_reconcile.json: $(cat "$RUN/lake/_quality/raw_reconcile.json" 2>/dev/null || echo 'KHÔNG CÓ')"
echo "== Sau 3 lần chạy 'hằng ngày' (mỗi ngày chỉ partition của ngày đó):"
api_up && get "/api/v1/revenue/daily?from=2026-10-15&to=2026-10-17"; api_down
STALE=$(java -jar "$JAR" stale --root="$RUN/lake" --from=2026-10-15 --to=2026-10-17) || fail "stale"
echo "== Ngày cũ theo lineage (job Scala biết, Dagster không): $STALE"
for day in $STALE; do
  materialize "rerun-$day" "staging_orders,daily_revenue,serving_daily_revenue" --partition "$day"
done
REMAINING=$(java -jar "$JAR" stale --root="$RUN/lake" --from=2026-10-15 --to=2026-10-17) || fail "stale sau chạy lại"
echo "== Ngày cũ còn lại: [$REMAINING]"
[ -z "$REMAINING" ] || fail "vẫn còn ngày cũ sau chạy lại"
echo "== Sau khi chạy lại ngày cũ:"
if api_up; then
  get "/api/v1/revenue/daily?from=2026-10-15&to=2026-10-17"
  get "/api/v1/revenue/monthly"
  echo "== GET /metrics"; curl --connect-timeout 2 --max-time 15 -fsS "http://127.0.0.1:$API/metrics" || fail "GET /metrics"
fi
api_down
[ "$FAILED" -eq 0 ] && echo "== KẾT QUẢ: mọi bước thành công" || echo "== KẾT QUẢ: có bước HỎNG — mã thoát 1"
exit "$FAILED"
