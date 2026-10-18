# Review T07 — Chương 18 + Project 2 (vòng 1)

Reviewer: Claude Code · Ngày: 17/10/2026 · Kết luận: **ĐẠT CÓ ĐIỀU KIỆN — 0 BLOCKER, 2 MAJOR, 6 MINOR.** Chất lượng rất tốt: các bài học
review chéo Ch15/Ch17 (ON CONFLICT, checked arithmetic, jti trong serverSecurityLogic, rate limit trong endpoint, exceptionHandler(None)) đều được giữ;
giới hạn được ghi trung thực.

## Kiểm chứng chạy thật (ngoài sandbox, máy lead)

`RUN_DB_TESTS=1 sbt.bat -batch test` trong `14-projects/p2-backend-service` — **exit 0, lần đầu, không sửa gì**:

```text
vn.scalaeng.p2.db.PostgresSuite: finished 21.928s
Test run vn.scalaeng.p2.db.PostgresSuite finished: 0 failed, 0 ignored, 12 total 21.951s
vn.scalaeng.p2.app.PostgresHttpSuite: finished 15.967s
Test run vn.scalaeng.p2.app.PostgresHttpSuite finished: 0 failed, 0 ignored, 1 total 15.968s
[info] Passed: Total 5 / 6 / 3 / 9 / 12 / 2   (theo module)  → 37 pass, 0 fail
[success] Total time: 70 s
```

PostgreSQL nhúng zonky 18.6.0 thật sự khởi động (21,9 s cho PostgresSuite, log HTTP→DB có 200/403/401 thật). Log đầy đủ: lead lưu cục bộ.
Docker vẫn chưa kiểm chứng (cần Docker Desktop trên máy người dùng).

## MAJOR

### M1 — Bài/README/handoff vẫn ghi "13 test DB assume-skip, chưa chạy"
Vị trí: `05-backend/ch18-backend-architecture.md` mục 3.3 (câu cuối), mục 9 (đoạn "24 test P2 pass ... 13 test DB assume-skip", câu cuối mục 9),
mục 10 ("không phải bằng chứng mới rằng đã chạy DB"); `14-projects/p2-backend-service/README.md` phần output/kiểm thử; `evidence/`.
Giờ đã có output thật: cập nhật thành **37/37 pass với sbt + PostgreSQL thật** (giữ câu giải thích assume-skip cho người chạy không bật `RUN_DB_TESTS`),
trích một đoạn output thật của PostgresSuite/PostgresHttpSuite (tự chạy lại được bằng scala-cli nếu sandbox cho; nếu không, dùng đoạn trích ở trên).
Ghi rõ phần còn chưa chạy: Docker build/up, SIGTERM container, `app/assembly` (lead sẽ chạy assembly).

### M2 — `GET /orders/{id}` khóa dòng `FOR UPDATE` trước khi kiểm tra quyền
Vị trí: `db/src/main/scala/vn/scalaeng/p2/db/PgStore.scala` (`getOrder` có `for update`), `service/.../Shop.scala` `order(...)`.
`getOrder` là thao tác ĐỌC của port Ch14 nhưng luôn lấy row lock. Hệ quả: (a) mọi GET đơn hàng tuần tự hóa với nhau và với hủy đơn;
(b) lock được lấy **trước** khi kiểm tra ownership — bất kỳ user đã đăng nhập nào cũng có thể gây tranh chấp khóa trên đơn của người khác (GET lặp lại
id của người khác chỉ nhận 403 nhưng vẫn khóa); (c) bài 3.3 dạy "getOrder khóa dòng order" như thể đọc cần khóa — sai thông điệp sư phạm về
transaction boundary. Đề xuất: tách `getOrder` (đọc thường) và `lockOrder` (FOR UPDATE, chỉ dùng trong `status`/`updateStatus`); thêm test
(ví dụ: một transaction giữ lock qua `status` chưa commit, GET cùng order vẫn trả ngay — hoặc ít nhất kiểm tra SQL không có `for update` ở đường đọc).
Cập nhật mục 3.3 và Ports docs.

## MINOR

- **m1 — BootstrapAdmin không nguyên tử.** `auth.register(...)` commit rồi mới `update users set role='Admin'` ở transaction khác. Chết giữa hai bước →
  còn lại tài khoản Customer với email admin, chạy lại thì bị từ chối ("không tự nâng quyền") → kẹt, phải sửa SQL tay. Băm mật khẩu trước, rồi
  `register + update role` trong MỘT `transact`.
- **m2 — `Passwords.policy` dùng `toLowerCase` theo locale máy** (2 chỗ). Dùng `Locale.ROOT` (bài học Ch19; chính `Checkout.product` đã làm đúng).
- **m3 — `RateLimiter` không bao giờ xóa bucket**: map tăng theo số IP khác nhau (IPv6 → không giới hạn thực tế). Bỏ bucket đã đầy (tokens = capacity)
  khi cập nhật, hoặc giới hạn kích thước; ghi trong README.
- **m4 — Không giới hạn kích thước body request.** `lines.size > 100` chỉ kiểm tra SAU khi decode toàn bộ JSON. Thêm giới hạn (Tapir
  `maxRequestBodyLength` trên endpoint, hoặc `EntityLimiter`/giới hạn tại server) và một test 413; nếu không làm, ghi thành giới hạn.
- **m5 — Metrics không đúng định dạng summary.** `http_request_duration_seconds_sum` không có `# TYPE` và không có `_count` tương ứng — Prometheus
  không tính được trung bình. Thêm `# TYPE http_request_duration_seconds summary` và `_count`.
- **m6 — Log test ở mức DEBUG.** Module `db`/`app` khi test không có `logback-test.xml` → log Tapir/http4s DEBUG tràn output sbt (hàng trăm dòng). Thêm
  `src/test/resources/logback-test.xml` (WARN) cho các module có test.

## Đã thăm dò, không phải lỗi
- Thứ tự khóa: checkout khóa product theo id tăng dần; hủy khóa order rồi product tăng dần; checkout không khóa order có sẵn → không có chu trình.
- Hoàn kho chỉ xảy ra Pending→Cancelled vì `canMoveTo` chỉ cho phép chuyển đó; retry cùng trạng thái không hoàn lại lần hai.
- Login: account không tồn tại vẫn verify dummy hash; role đọc từ DB mỗi request; `Authorization` được http4s Logger che.
- Phân trang một câu lệnh (count + items cùng snapshot), sort luôn có `id` làm khóa phụ.

## Để đóng task
Sửa M1, M2, m1–m6 trong phạm vi brief T07; chạy lại scala-cli test (phần không cần DB) + lời giải; ghi "Vòng 2" trong handoff. Lead chạy lại
`RUN_DB_TESTS=1 sbt test` + `app/assembly` rồi commit.
