# T27 — Tài liệu thư viện theo phase (tác giả: Codex; reviewer/lead: Claude)

## Mục tiêu
Mỗi phase có một tài liệu mô tả CHI TIẾT các thư viện/công cụ mà giáo trình thực sự dùng, để người học tra cứu nhanh mà không phải lục từng chương.

## File được phép ghi
`libraries/phase-03-scala-core.md`, `libraries/phase-04-backend.md`, `libraries/phase-05-data-spark.md`, `libraries/phase-06-streaming.md`,
`libraries/phase-07-data-platform.md`, `libraries/phase-08-projects.md`, `libraries/tooling.md` (sbt, Scala CLI, Coursier, munit, scalacheck, JDK,
Docker, WSL2 — công cụ dùng xuyên suốt), `.team/handoffs/T27-libraries.md`, `.team/progress/T27.md`, `.cache/`.
KHÔNG ghi `libraries/README.md` (lead viết chỉ mục), file dùng chung, chương, lab.

## Nguồn danh sách thư viện
Quét `//> using dep` / `test.dep` trong mọi `project.scala` và `libraryDependencies`/`addSbtPlugin` trong mọi `build.sbt`/`project/plugins.sbt`
của repo; gom theo phase dựa vào thư mục (03-type-system… → phase 3; 05-backend, 06-database, 07-concurrency, P2 → phase 4; 08-data-processing,
09-spark, P3 → phase 5; 10-streaming, P4 → phase 6; 11-data-platform, 13-performance → phase 7; P1–P5 phần riêng → phase 8). Thư viện xuất hiện ở
nhiều phase: mô tả đầy đủ ở phase dùng lần đầu, phase sau chỉ ghi một dòng + liên kết. Ngoài Maven: Dagster (Python, phase 7), PostgreSQL, Kafka broker.

## Khung bắt buộc cho MỖI thư viện (tiếng Việt có dấu, giọng sư phạm)
1. **Là gì / giải quyết vấn đề gì** (2–4 câu, vì sao cần nó trong backend/data engineering).
2. **Phiên bản trong giáo trình** + artifact chính xác + link POM Maven Central (hoặc PyPI/trang release) + ngày kiểm chứng; ghi rõ Scala 3 hay chỉ 2.13
   (KHÔNG suy hỗ trợ Scala 3 từ Scala 2; Spark chỉ 2.13 — xem 00-roadmap/04-compatibility-matrix.md).
3. **Khái niệm cốt lõi** (các kiểu/hàm chính, mô hình hoạt động).
4. **Ví dụ** — CHỈ trích code đã có trong lab/solution đã chạy test (5–15 dòng), ghi `đường/dẫn/file.scala:dòng`. Không viết code mới chưa chạy.
5. **Lựa chọn thay thế** và khi nào chọn cái nào (ưu/nhược).
6. **Lỗi/bẫy thường gặp** — chỉ dẫn lỗi THẬT đã có trong mục 10 của chương/README (trích nguyên văn + liên kết); không bịa thông báo lỗi.
7. **Dùng ở đâu trong giáo trình** (chương/lab/project, liên kết tương đối).
8. **Tài liệu chính thức** (link).
Đầu mỗi file: dòng tiêu đề `# Thư viện — Phase N: ...`, blockquote phiên bản + trạng thái kiểm chứng, bảng tóm tắt (thư viện | phiên bản | Scala 3 | vai trò | chương).

## Tiêu chí xong
- Mọi dependency trong build của phase có mặt (handoff liệt kê bảng đối chiếu build ↔ tài liệu, không thiếu, không thừa).
- Mọi `file:dòng` trỏ đúng; mọi liên kết local hợp lệ (kiểm bằng script); link Maven Central kiểm HTTP nếu sandbox cho phép, không được thì ghi "chưa kiểm HTTP".
- Không mục nào chỉ có tiêu đề. File UTF-8.
