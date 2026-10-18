# Review T06 — Chương 21 Data Formats (reviewer: Claude Code)

## Kiểm chứng của reviewer (17/10/2026, ngoài sandbox)
- `sbt.bat -batch test` trong `08-data-processing/formats-lab`: **Passed: Total 25, Failed 0** → build.sbt đã kiểm chứng.
- `scala-cli test solutions/ch21-code`: 12/12.
- Kỹ thuật rất tốt: bảng schema evolution 8 biến thể **kiểm chứng bằng ghi file v1/đọc v2 và ngược lại** (không suy từ case class); benchmark 1 triệu
  đơn hàng có warm-up, 3 lượt, median, lưu raw; tách "byte qua API" khỏi "I/O đĩa vật lý" ở projection. Chọn parquet-avro + hadoop-client shaded
  chạy được trên Windows là thông tin quý.

## BLOCKER
(không có)

## MAJOR
1. **Bài không có một dòng code Scala nào** (0 khối ```scala trong 44 KB; chỉ powershell/text/mermaid). Đây cũng là MAJOR đầu tiên của T01 và T03 —
   người học không thấy Avro schema, writer/reader, Parquet writer, projection, codec JSON trông thế nào. Bổ sung đoạn code ngắn TRÍCH từ source đã
   biên dịch (5–20 dòng, có chú thích) cho tối thiểu:
   - 3.1/5: một round-trip JSON Lines và CSV (encode + decode một `Order`), chỉ ra chỗ xử lý null/rỗng/`\N`.
   - 3.2: ghi + đọc Avro (`DataFileWriter`/`GenericRecord` hoặc cách lab làm), và ghi + đọc Parquet qua `AvroParquetWriter`/`LocalOutputFile`
     (hoặc API lab dùng) — kèm dòng chọn codec nén.
   - 3.3: **schema Avro v1 và ít nhất 3 biến thể v2 dưới dạng JSON** (default, alias, union null) + đoạn code kiểm tra compatibility
     (`SchemaCompatibility...`) và đọc file v1 bằng reader v2.
   - 3.4: phân biệt thiếu / null / rỗng trong codec (circe) — một đoạn decoder.
   - 3.5: Money/decimal (logical type `decimal`), timestamp-millis, chuyển timezone khi tính ngày.
   - 3.6: projection Parquet (đọc một cột) — schema projection được truyền vào reader như thế nào.
   - 6.2: khung `Benchmark` (warm-up, lượt đo, median) để người học hiểu cách đo đúng.
2. Kiểm tra lại văn phong 3.1–3.7 và mục 8 cho người mới (như T03 vòng 2): tách câu dài nhiều ý, giải thích "vì sao" trước "làm thế nào". Giữ nguyên
   mọi khẳng định đã kiểm chứng; có thể tăng độ dài bài.

## MINOR
3. Dòng 1: khối phiên bản đặt TRƯỚC tiêu đề `#` — đưa tiêu đề lên đầu như các chương khác; cập nhật trạng thái "sbt 25/25 do reviewer chạy, đã review chéo".
4. Thêm mục "Vị trí trong lộ trình" (sau Ch19 — JSONL/CSV bằng thư viện; trước Ch22 Spark — Spark đọc Parquet/Avro, nhánh Spark dùng Scala 2.13 nên
   ranh giới hai thế giới là **file Parquet/Avro**, như compatibility matrix mục 5).
5. Mục 7: lệnh chạy chính cho người học là `cd 08-data-processing/formats-lab && scala-cli test .` / `sbt test`; `COURSIER_CACHE`, `--server=false`,
   `--suppress-outdated-dependency-warning` là ghi chú phụ.
6. Đề xuất cho compatibility matrix (lead sẽ áp dụng): Avro 1.12.2, parquet-avro 1.18.1 + hadoop-client-api/runtime 3.4.2 (shaded) chạy trên Windows
   không cần winutils — xác nhận điều này trong handoff (đã chạy trên Windows chưa? cần `HADOOP_HOME` không?).

## Phạm vi sửa
Như brief T06; ghi mục "Vòng 2" trong handoff; chạy lại test Scala CLI.
