# Lời giải — Chương 9, 10, 11

> **Trạng thái kiểm chứng:** code lời giải trọng tâm (Ch9 bài 2, Ch10 bài 1 & 5, Ch11 bài 5) **đã biên dịch và chạy** với `scalac 3.9.0` trên classpath của `order-platform`;
> các số liệu Ch11 bài 1, 2, 4 là **output thật** của máy biên soạn (1 CPU, Linux, OpenJDK 21.0.10). Ch9 bài 5 và Ch11 bài 3 cần bạn tự chạy (cần Internet / số liệu phụ thuộc máy).

## Chương 9 — Project Structure

**Bài 1 — `REPORT_PREFIX`.** Thêm field `reportPrefix: String` vào `AppConfig`, `val prefixE = Right(vars.getOrElse("REPORT_PREFIX", "").trim)`, đưa vào tuple tích lũy lỗi.
Trong `Main`: `cfg.outputDir.resolve(s"${cfg.reportPrefix}daily_revenue.csv")`. Test: không khai báo → `""`; khai báo `"2026-10-14_"` → tên file đúng.
Lưu ý bảo mật: prefix có `/` hoặc `..` có thể ghi file ra ngoài thư mục output (path traversal) — nên validate `prefix.matches("[A-Za-z0-9_-]*")`.

**Bài 2 — `job_id` cho mọi log (decorator).**

```scala
final class WithFields(inner: Logger, extra: (String, String)*) extends Logger:
  def info(m: String, f: (String, String)*): Unit  = inner.info(m, (extra ++ f)*)
  def warn(m: String, f: (String, String)*): Unit  = inner.warn(m, (extra ++ f)*)
  def error(m: String, f: (String, String)*): Unit = inner.error(m, (extra ++ f)*)
```

Output thật khi bọc một logger ghi vào bộ nhớ: `List(INFO finished (job_id,j-1),(valid,4))`.
Trong `Main`: `val log = WithFields(JsonLogger(...), "job_id" -> env.getOrElse("JOB_ID", "local"))`. `(xs)*` truyền một `Seq` vào tham số varargs.
Mẫu **decorator** này là lý do định nghĩa `Logger` là trait nhỏ: thêm hành vi xuyên suốt mà không sửa chỗ gọi.

**Bài 3 — module `report`.** Sơ đồ: `domain ← core ← report ← app`. `report` chỉ biến `PipelineResult` thành chuỗi CSV (thuần). Nếu `report` phụ thuộc `app`
thì `app` (đã phụ thuộc `report`) tạo vòng — sbt từ chối; quan trọng hơn, logic định dạng sẽ dính vào cấu hình/I/O.

**Bài 4 — `FAIL_ON_DUPLICATES`.** Thêm `ExitCode.DuplicatesFound = 4`. Thứ tự kiểm tra trong `Main` là một **quyết định**: kiểm tra tỷ lệ lỗi trước (exit 2) rồi mới đến trùng (exit 4),
hay ngược lại — ghi vào README và test cả trường hợp có cả hai.

**Bài 5 — `Slf4jLogger`.** Hướng dẫn:

```scala
final class Slf4jLogger(underlying: org.slf4j.Logger) extends Logger:
  private def fmt(m: String, f: Seq[(String, String)]) = (m +: f.map((k, v) => s"$k=$v")).mkString(" ")
  def info(m: String, f: (String, String)*): Unit  = underlying.info(fmt(m, f))
  def warn(m: String, f: (String, String)*): Unit  = underlying.warn(fmt(m, f))
  def error(m: String, f: (String, String)*): Unit = underlying.error(fmt(m, f))
```

Trạng thái: **chưa kiểm chứng** (cần `slf4j-api 2.0.18`, `logback-classic 1.6.0`). SLF4J 2.x còn có fluent API `underlying.atInfo().addKeyValue(k, v).log(m)` cho log có cấu trúc thật sự.

**Câu hỏi kiểm tra — gợi ý:** (1) core sẽ không test được thuần và không dùng lại được trong ngữ cảnh khác (consumer Kafka, API); (2) `aggregate` chỉ gộp lệnh, `dependsOn` là phụ thuộc classpath;
(3) sbt 1.x biên dịch build bằng Scala 2.12; (4) build: `-Werror` khi CI, runtime: `INPUT_PATH`; (5) đoán sai đường dẫn ở prod có thể xử lý nhầm dữ liệu — dừng an toàn hơn;
(6) máy lọc/tổng hợp được theo field; (7) exit 2 cần người xem dữ liệu, không retry; exit 3 có thể retry có giới hạn; (8) mã không kiểm soát, xung đột phiên bản, lỗ hổng bảo mật.

## Chương 10 — Testing

**Bài 1 — `Quantity.parse` boundary.** Kiểm chứng thật: `"+3".toIntOption == Some(3)` (Java `Integer.parseInt` chấp nhận dấu `+`), `"2147483648".toIntOption == None`.
Vậy `"+3"` đang **hợp lệ**. Có nên không? Với dữ liệu từ hệ thống khác, chấp nhận `+3` thường vô hại; nếu muốn chặt chẽ, thêm `raw.trim.forall(_.isDigit)`.
Điều quan trọng: **quyết định và ghi bằng test** — đừng để hành vi tình cờ của thư viện thành đặc tả ngầm.

**Bài 2.** `def alphaNumString(maxLen: Int): Gen[String] = stringOf(maxLen, oneOf('a', 'Z', '0', '9', 'm'))`; property: `s.nonEmpty && s.length <= 32 ==> Sku.parse(s).isRight`
(lọc input bằng điều kiện trong property: `!(s.nonEmpty && s.length <= 32) || Sku.parse(s).isRight`).

**Bài 3.** `r.daily.map(_.orders).sum == r.valid.size`. Khi cố ý cộng nhầm `rejected` vào `orders`, property fail ngay với seed 42 trên một list có dòng lỗi — chứng minh property thật sự kiểm tra điều gì đó.

**Bài 4.** Generator `padded(row) = row.split(",", -1).map(f => s"  $f ").mkString(",")` — lưu ý trường có dấu nháy kép phải xử lý riêng (khoảng trắng ngoài dấu nháy). Property: `run(header :: rows).map(_.valid) == run(header :: rows.map(padded)).map(_.valid)`.
Nếu property fail với trường có nháy kép, đó là phát hiện thật về giới hạn của `CsvLine` — ghi lại hoặc sửa.

**Bài 5 — shrinking.**

```scala
object Shrink:
  def forAllList[A](gen: Gen[List[A]], runs: Int = 200, seed: Long = 42L)(p: List[A] => Boolean): Option[List[A]] =
    def fails(xs: List[A]) = !Try(p(xs)).getOrElse(false)
    def shrink(xs: List[A]): List[A] =
      xs.indices.iterator.map(i => xs.patch(i, Nil, 1)).find(fails) match
        case Some(smaller) => shrink(smaller)
        case None          => xs
    val rnd = Random(seed)
    Iterator.range(0, runs).map(_ => gen.sample(rnd)).find(fails).map(shrink)
```

Output thật với property sai về số dấu phẩy:

```
không shrink: Some(List(a;a" , "đđ, ,a; ))
có shrink:   Some(List(,a; ))
```

Từ 2 trường xuống **1 trường** chứa dấu phẩy — lý do lỗi hiện ra ngay. ScalaCheck còn thu nhỏ **từng phần tử** (chuỗi ngắn lại), nên phản ví dụ thường còn gọn hơn nữa.

**Câu hỏi kiểm tra — gợi ý:** (1) logic thuần test được không cần môi trường; (2) round-trip, invariant, conservation, determinism (xem bảng 3.4); (3) để tái hiện; (4) stub: đồng hồ cố định,
fake: luồng log trong bộ nhớ và thư mục tạm, spy: đọc log đã ghi; (5) seam là tham số/trait nhỏ, fake viết tay đủ; (6) thay đổi định dạng toàn cục, thứ tự, escape;
(7) test đỏ → sửa → test xanh → commit cùng nhau; (8) SQL dialect và hành vi khác PostgreSQL.

## Chương 11 — JVM Fundamentals

**Bài 1.** Output thật với `-Xmx256m -XX:ActiveProcessorCount=2 -XX:+UseSerialGC`:

```
CPU khả dụng: 2
Heap tối đa (-Xmx): 247 MB
GC: Copy, MarkSweepCompact
JVM args: -Xmx256m -XX:ActiveProcessorCount=2 -XX:+UseSerialGC
```

- `ActiveProcessorCount=2` làm JVM **tin** có 2 CPU (máy thật có 1) → ảnh hưởng số thread mặc định của thread pool, GC, `ForkJoinPool`. Dùng khi container báo sai số CPU.
- Heap báo 247 MB < 256 MB vì `maxMemory()` không tính một vùng survivor.
- `Copy, MarkSweepCompact` là hai collector của Serial GC (vùng trẻ / vùng già). Không có cờ `UseSerialGC`, JVM cũng chọn Serial vì chỉ thấy 1 CPU (bài học), nhưng **với `ActiveProcessorCount=2` và bộ nhớ đủ lớn, JVM có thể chọn G1** — vì vậy cờ GC tường minh giúp hành vi ổn định giữa các môi trường.

**Bài 2.** Output thật:

```
-Xss256k: ~2989     (bài học)
-Xss512k: ~8451
-Xss1m:   ~19373
-Xss2m:   ~19161
-Xss4m:   ~84909    (bài học)
```

Độ sâu **không tỷ lệ thuận** với `-Xss` (1m và 2m gần bằng nhau). Lý do: kích thước frame của `depth` **thay đổi trong lúc chạy** — frame của code đang thông dịch lớn hơn frame của code đã được JIT biên dịch,
và thời điểm JIT kích hoạt khác nhau giữa các lần chạy. Ước lượng thô: 1 MB / ~19.000 ≈ 50–55 byte/frame cho frame đã biên dịch; 256 KB / ~3.000 ≈ 85 byte/frame khi phần lớn còn thông dịch.
Bài học: **đừng dựa vào độ sâu đệ quy cụ thể** — dùng đệ quy đuôi hoặc vòng lặp (Chương 4); tăng `-Xss` chỉ là giải pháp tạm.

**Bài 3.** Hướng dẫn: `Array[Int].sum` dùng `Numeric[Int]` qua phương thức generic — mỗi phần tử có thể bị boxing tùy cách JIT tối ưu; vòng `while` trên `Vector[Int]` vẫn phải unbox từng `Integer`.
Thứ tự thường thấy: `while` trên `Array` nhanh nhất; `while` trên `Vector` nhanh hơn `foldLeft` nhưng chậm hơn `Array`; số liệu cụ thể phụ thuộc máy — hãy dùng JMH (`sbt-jmh`) cho phép đo đáng tin.

**Bài 4.** Số dòng `Pause` thật: `-Xmx32m` → **138**; `-Xmx512m` → **55**. Heap lớn → vùng trẻ lớn hơn → ít lần đầy → ít pause. Cái giá: mỗi pause có thể **dài hơn**
(nhiều dữ liệu sống phải sao chép/quét hơn), tốn RAM của máy/container, và khởi động đầy đủ heap chậm hơn. Mục tiêu tuning là cân bằng tần suất và độ dài pause theo SLA.

**Bài 5 — tránh deadlock.**

```scala
// (a) Lấy khóa theo thứ tự toàn cục
def transferOrdered(from: AnyRef, to: AnyRef): Unit =
  val (l1, l2) = if System.identityHashCode(from) <= System.identityHashCode(to) then (from, to) else (to, from)
  l1.synchronized { Thread.sleep(50); l2.synchronized { () } }

// (b) tryLock có timeout, nhả khóa và thử lại
def transferTry(first: ReentrantLock, second: ReentrantLock): Boolean =
  if first.tryLock(100, TimeUnit.MILLISECONDS) then
    try
      Thread.sleep(50)
      if second.tryLock(100, TimeUnit.MILLISECONDS) then try true finally second.unlock()
      else false
    finally first.unlock()
  else false
```

Output thật: `ordered: t1 alive=false, t2 alive=false` và `tryLock: done1=true done2=true, alive=false` — cả hai cách đều kết thúc.
(a) loại bỏ deadlock tận gốc; (b) chỉ tránh treo vĩnh viễn — có thể gây *livelock* (hai bên cùng nhả và thử lại mãi) nếu không có độ trễ ngẫu nhiên khi thử lại.
Trong backend thật: hai transaction cập nhật `accounts` theo thứ tự khác nhau (`UPDATE ... WHERE id = A` rồi `B` và ngược lại) → PostgreSQL phát hiện deadlock và hủy một transaction.
Cách phòng tương tự (a): luôn cập nhật theo thứ tự id tăng dần (Chương 15).
