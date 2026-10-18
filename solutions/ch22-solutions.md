# Lời giải — Chương 22: Spark Fundamentals

> Code: [`ch22-code/`](ch22-code/) — Scala 2.13.18 + Spark 4.2.0, dùng lại `09-spark/spark-fundamentals-lab/src/main` qua `//> using file`.
> Chạy: `cd solutions/ch22-code && scala-cli test .` → **8/8 test pass** (17/10/2026, Windows 11 + JDK 21). Số đo dưới đây là output thật của test.

## Bài 1 — Mô hình Chương 15 bằng Scala 2.13

```scala
sealed trait OrderStatus extends Product with Serializable { def dbName: String }
object OrderStatus {
  case object Pending extends OrderStatus { val dbName = "PENDING" }
  ...
  val values: List[OrderStatus] = List(Pending, Paid, Cancelled)
}
final case class OrderId(value: Long) extends AnyVal          // thay opaque type
implicit class OrderOps(private val o: Order) extends AnyVal { def isOpen: Boolean = ... }   // thay extension
```

- `sealed`: compiler biết mọi trường hợp → cảnh báo match thiếu (như `enum`). `extends Product with Serializable` để kiểu suy luận của `List(Pending, Paid)` là
  `List[OrderStatus]` thay vì `List[Product with Serializable with OrderStatus]`.
- Scala 2.13 **không tự sinh** `values` cho sealed trait — phải liệt kê tay (dễ quên khi thêm trạng thái mới; có thư viện enumeratum giải quyết, ngoài phạm vi).
- Value class `extends AnyVal`: không cấp phát object trong đa số trường hợp, nhưng **bị boxing** khi nằm trong collection generic, khi pattern match, hoặc khi dùng như
  kiểu tổng quát — khác `opaque type` của Scala 3 (không bao giờ có wrapper lúc chạy).

## Bài 2 — Dự đoán số stage

| Job | Dự đoán | Đo thật | Giải thích |
|---|---|---|---|
| `select` + `write.parquet` | 1 | **1** | Chỉ narrow; mỗi task ghi file của partition mình |
| `distinct().count()` | 3 | **3** | `distinct` = groupBy mọi cột (shuffle 1), `count` = tổng hợp (shuffle 2) → 3 stage |
| `join` (tắt broadcast) + `count` | 4 | **4** | Sort-merge join: shuffle **cả hai** phía theo khóa (2 stage đọc nguồn), stage join, rồi `count` shuffle thêm → stage cuối |

Output thật:

```
[bài 2] stage: write=1, distinct+count=3, SortMergeJoin+count=4
[bài 2] maxShuffledHashJoinLocalMapThreshold=128MB -> ShuffledHashJoin, 4 stage
```

- `spark.sql.autoBroadcastJoinThreshold = -1` **tắt broadcast tự động** — nhưng **không buộc** sort-merge join. Review chéo chỉ ra: giữ broadcast tắt, đặt
  `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold = 128MB` → plan **ban đầu** là SortMergeJoin, plan **cuối** sau khi AQE chạy là **ShuffledHashJoin**.
  Bản đầu chỉ đếm stage nên không phát hiện được (cả hai đều 4 stage).
- Vì vậy `joinWithoutBroadcast` (1) cố định mọi cấu hình liên quan (`autoBroadcastJoinThreshold`, `adaptive.autoBroadcastJoinThreshold`,
  `join.preferSortMergeJoin`, `adaptive.maxShuffledHashJoinLocalMapThreshold`), (2) đọc chiến lược từ **plan cuối** — `AdaptiveSparkPlanExec.executedPlan`.
  Bẫy thật khi viết: `toString` của plan AQE sau khi chạy in **cả** "Final Plan" lẫn "Initial Plan", nên tìm chuỗi trên toàn bộ luôn thấy `SortMergeJoin` của plan
  ban đầu — test đỏ với `obtained: SortMergeJoin` cho đến khi lấy riêng plan cuối.
- Mặc định (10 MB) bảng nhỏ được **broadcast** — gửi nguyên bảng nhỏ tới mọi executor, không shuffle phía bảng lớn → ít stage hơn. Chương 25 bàn chọn chiến lược join.
- Test đòi `write == 1` và các job còn lại **≥** con số tối thiểu, vì AQE có thể đổi cách chia stage giữa các phiên bản Spark.

## Bài 3 — Cache khi một Dataset dùng cho hai nhánh

| | Số lần `map` chạy (100.000 dòng) |
|---|---|
| Không cache, 2 nhánh `groupBy("campaign")` và `groupBy("day")` | **200.000** |
| `cache()` | **100.000** |

- Không cache, mỗi nhánh là một job riêng, mỗi job tính lại `clean` từ đầu theo lineage.
- Lỗi thật khi viết: `spark.range` trả **`Dataset[java.lang.Long]`**, không phải `Dataset[Long]` của Scala; `.map { x => x }(Encoders.scalaLong)` báo
  `found: Encoder[scala.Long], required: Encoder[java.lang.Long]`. Sửa: `import spark.implicits._` và trả `x.longValue`.

## Bài 4 — `spark.sql.shuffle.partitions` khi tắt AQE

| `shuffle.partitions` | Partition sau shuffle | Thời gian (groupBy 1 triệu dòng → 100 khóa, local[2]) |
|---|---|---|
| 200 (mặc định) | **200** | **1.760 ms** |
| 4 | **4** | **307 ms** |

- Tắt AQE, Spark dùng đúng số đã cấu hình: 200 task cho 100 khóa — phần lớn rỗng, mỗi task vẫn tốn chi phí lập lịch, mở file shuffle. Chậm gấp ~5,7 lần ở đây.
- Bật AQE (mặc định Spark 4), Spark tự gộp (Chương 22 mục 3.5: 200 → 1). Đây là lý do AQE được bật mặc định từ Spark 3.2.
- Số đo trên **2 core**; trên cụm lớn với dữ liệu lớn, 4 partition lại là quá ít (mỗi task xử lý quá nhiều dữ liệu, tràn bộ nhớ). Không có con số đúng cho mọi job —
  Chương 25 bàn cách chọn theo kích thước dữ liệu (~100–200 MB mỗi partition).

## Bài 5 — Lỗi dữ liệu không cứu được bằng chạy lại

```
Left(Job aborted due to stage failure: Task 0 in stage 33.0 failed 3 times, most recent failure: Lost task 0.2 in stage 33.0 (TID 257)
(nitro-5 executor driver): java.lang.IllegalArgumentException: dòng xấu: 42 (lần thử 2)); số lần thử = 3
```

- `local[2,3]`: thử tối đa 3 lần. Dòng 42 lỗi ở **mọi** lần thử (lần thử 0, 1, 2 — `task 0.2` là lần thử thứ 3) → job bị hủy.
- So với Chương 22 mục 3.6: lỗi **hạ tầng** (executor chết) qua được nhờ chạy lại; lỗi **dữ liệu** thì không — chạy lại chỉ tốn thời gian.
- Cách đúng: kiểm tra dữ liệu **trước** (Chương 19–20: dòng xấu thành giá trị `Left`/vào bảng rejected), không để exception phá cả job. Chương 24 làm điều này trong Spark.

## Bài 6 — Chương 20 bằng RDD API

```scala
spark.sparkContext.parallelize(rows, 4)
  .mapPartitions(it => it.toSeq.groupMapReduce(_._1)(_._2)(_ + _).iterator)   // (b) tổng hợp cục bộ
  .reduceByKey(_ + _)                                                         // (c)+(d) shuffle theo khóa rồi gộp
```

- `mapPartitions`: hàm nhận **cả partition** (một iterator) — tổng hợp cục bộ như bước (b) của `Execution.partitioned` (Chương 20).
- `reduceByKey` tự làm map-side combine **và** shuffle; tự viết `mapPartitions` ở đây chỉ để thấy rõ các bước. Thực tế chỉ cần `reduceByKey`.
- `reduceByKey(f)` đòi `f` **kết hợp và giao hoán** (thứ tự các phần không xác định) — đúng điều kiện Monoid + giao hoán ở Chương 20.
- Kết quả bằng `groupBy("sku").agg(sum(...))` của DataFrame. Ưu tiên DataFrame: Spark tối ưu được (Chương 23); RDD là "hộp đen" với optimizer.

**Tràn số — hai cách "giống nhau" khác hành vi (review chéo).** Bản đầu cộng `Long`. Với A = `Long.MaxValue` và `1`:

| | Bản `Long` |
|---|---|
| RDD `_ + _` | `-9223372036854775808` — tràn **lặng lẽ**, doanh thu âm |
| DataFrame `sum` | `SparkArithmeticException: [ARITHMETIC_OVERFLOW] overflow. Use 'try_add' to tolerate overflow...` — ANSI bật mặc định ở Spark 4 |

"Kết quả bằng nhau" chỉ đúng khi tổng vừa `Long`. Lời giải chọn **một chính sách chung**: tổng là số nguyên không tràn — `BigInt` ở RDD,
`cast("decimal(38,0)")` trước `sum` ở DataFrame — giống bài học `Stats.sum: BigInt` của Chương 20. Test kiểm tra tổng vượt `Long` theo cả hai chiều, nhiều
partition, input rỗng, và giữ bản `Long` của DataFrame để thấy ANSI ném lỗi. **Không** tắt ANSI để hai bên "khớp" — cả hai sẽ cùng trả số sai.
