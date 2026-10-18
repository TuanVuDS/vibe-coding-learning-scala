# Handoff T08 — Chương 19: Scala cho Data Processing (tác giả: Claude Code)

## File
- `08-data-processing/ch19-scala-data-processing.md` (17 mục)
- `08-data-processing/ingest-lab/` (project.scala + build.sbt; data/, expected/report.csv tính tay; 15 test)
- `solutions/ch19-solutions.md`, `solutions/ch19-code/` (6 test)

## Kiểm chứng (17/10/2026)
- scala-cli test + sbt test: 15/15 (NormalizeSuite 7, PipelineSuite 8). Lời giải 6/6.
- IngestMain trên data mẫu: Summary(17,8,7,2,1,3), report.csv khớp golden tính tay từng byte.
- Thí nghiệm 1 triệu dòng (164,8 MiB) ở -Xmx128m/256m/1g: bảng mục 3.5 là số thật.
- Mutation test: bỏ kiểm tra campaign của click đến sớm → 2 test đỏ (đã khôi phục).
- Ví dụ mục 5 đã chạy thật.

## Phiên bản
fs2-io 3.13.0 (bản 3.14.0 có; giữ 3.13.0 khớp fs2-data-csv 1.14.1) · fs2-data-csv(-generic) 1.14.1 · circe-parser 0.14.17.

## Điểm cần reviewer soi kỹ
- Order-invariance của báo cáo: dedup giữ bản ĐẦU TIÊN → hai bản `c6` khác ts 5 giây; nếu ts khác NGÀY thì báo cáo phụ thuộc thứ tự (chưa có test cho trường hợp này).
- `Pipeline.runAll` dùng `acc ++ rejs` trên Vector (O(n) tổng) — chấp nhận cho test.
