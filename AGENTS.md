# AGENTS.md — Quy ước làm việc nhóm (Claude Code + Codex)

Repo này là giáo trình tiếng Việt **Scala 3 cho Backend & Data Engineering**.
Đề bài gốc: `PROMPT_PROJECT.md` (đọc kỹ PHẦN 11–14). Tiến độ: `PROGRESS.md`.

## 1. Vai trò

| Agent | Vai trò |
|---|---|
| **Claude Code** (lead) | Lập kế hoạch, giao task, review chéo, chạy kiểm chứng cuối, là người DUY NHẤT sửa các file dùng chung: `README.md`, `PROGRESS.md`, `00-roadmap/*`, `glossary/glossary.md`, `exercises/README.md`, `AGENTS.md`, và commit git. |
| **Codex** (co-author) | Biên soạn chương/project được giao trong brief, review chéo chương của Claude khi được yêu cầu. |

Mỗi chương có **một tác giả** và **một reviewer là agent còn lại**. Không chương nào được đánh dấu ✅
khi chưa qua review chéo VÀ chưa chạy test thật.

## 2. Luồng một task

1. Lead viết brief: `.team/briefs/<task-id>.md` (phạm vi, thư mục được phép ghi, tiêu chí xong).
2. Tác giả làm việc **chỉ trong thư mục được giao** (+ `solutions/` file tương ứng).
3. Tác giả viết báo cáo bàn giao: `.team/handoffs/<task-id>.md` gồm:
   - Danh sách file đã tạo/sửa.
   - Lệnh build/test đã chạy + số test pass/fail (dán output tóm tắt THẬT).
   - Phiên bản thư viện đã chốt + link nguồn đã kiểm chứng + ngày kiểm chứng.
   - Đề xuất cập nhật cho `PROGRESS.md`, `glossary`, `compatibility-matrix` (lead sẽ áp dụng).
   - Những gì CHƯA kiểm chứng được và lý do.
4. Reviewer viết `.team/reviews/<task-id>-review.md`: danh sách vấn đề theo mức `BLOCKER / MAJOR / MINOR`,
   mỗi vấn đề có file:dòng và đề xuất sửa. Reviewer không tự sửa file của tác giả trừ lỗi chính tả nhỏ.
5. Tác giả sửa hết BLOCKER/MAJOR → lead chạy lại test → cập nhật file dùng chung → commit.

## 3. Chuẩn nội dung (bắt buộc)

- Viết tiếng Việt có dấu, giọng sư phạm; thuật ngữ tiếng Anh giữ nguyên kèm giải thích lần đầu.
- Mỗi chương theo **đủ 17 mục** của `00-roadmap/08-lesson-template.md`; không để mục chỉ có tiêu đề.
- Mỗi chủ đề quan trọng: vấn đề → vì sao → cách hoạt động → lựa chọn thay thế → ưu/nhược → khi nào dùng → lỗi thiết kế thường gặp.
- Dòng đầu chương ghi: phiên bản Scala, JDK, dependency + version, trạng thái kiểm chứng.
- **Không bịa**: mọi output trích trong bài phải là output thật đã chạy; mọi thông báo lỗi trong
  "Lỗi thường gặp" phải là thông báo thật của compiler/runtime. Không chắc → ghi `Chưa kiểm chứng`.
- Phiên bản thư viện: kiểm chứng trên trang chính thức / Maven Central / GitHub releases, ghi link + ngày.
  Không suy ra hỗ trợ Scala 3 từ việc hỗ trợ Scala 2. Spark chỉ dùng Scala 2.13 (xem compatibility matrix).
- Không dạy frontend. Văn phong và ví dụ nối tiếp các chương trước (domain đơn hàng/người dùng/sự kiện).
- Không tạo file rỗng hoặc chỉ có tiêu đề.

## 4. Chuẩn code

- Scala **3.9.0** (trừ nhánh Spark: 2.13.x), JDK 21, munit 1.2.4. Package `vn.scalaeng.<module>`.
- Mỗi lab có `project.scala` (Scala CLI directives) và, khi là project nhiều module, `build.sbt` + `project/build.properties` (sbt 1.12.11).
- Bật `-deprecation -feature -Wunused:all`; mục tiêu: **0 cảnh báo**.
- Ví dụ quan trọng phải có test munit. Không cần mạng/Docker để chạy unit test; test cần PostgreSQL/Kafka/Docker
  phải tách riêng và tự bỏ qua (`assume`) khi thiếu môi trường, ghi rõ trong bài.
- Ưu tiên code dễ đọc trước khi tối ưu.

## 5. Lệnh kiểm chứng trên máy này (Windows)

```
scala-cli.bat test <thư-mục> --server=false      # Codex: BẮT BUỘC --server=false (sandbox chặn bloop)
scala-cli.bat run  <thư-mục> --server=false --main-class <fqcn>
sbt.bat test                                      # trong thư mục có build.sbt
```

Trong sandbox Codex: đặt `$env:COURSIER_CACHE = <project>/.cache/coursier` (thư mục `.cache/` phải được ignore); sbt launcher bị chặn (`BootServerSocket` AccessDenied) → kiểm chứng bằng Scala CLI, Claude chạy `sbt test`.
`munit-cats-effect`: dùng **2.2.0** (2.2.1 kéo munit 1.3.6, lệch chuẩn munit 1.2.4).

Công cụ cài qua Coursier tại `C:\Users\VuTT\AppData\Local\Coursier\data\bin`. Có Internet. Docker có cài
nhưng daemon có thể chưa chạy — kiểm tra trước khi dựa vào.

## 6. Không được làm

- Không sửa file ngoài phạm vi brief (đặc biệt các file dùng chung ở mục 1).
- Không `git commit`, `git reset`, `git checkout --`, xóa file của agent khác.
- Không đánh dấu "đã kiểm chứng" cho thứ chưa chạy.
