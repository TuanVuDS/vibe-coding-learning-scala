# Review chéo T02 — Chương 13 (Codex)

Ngày review: **17/10/2026**. Đã đọc handoff, toàn bộ bài học, nguồn và test của hai lab, lời giải Markdown và code lời giải. Chỉ ghi file review này và dữ liệu kiểm chứng trong `.cache/`.

**Kết luận: cần sửa 2 MAJOR, 2 MINOR; không có BLOCKER.**

## BLOCKER

Không phát hiện.

## MAJOR

### M1 — Handler chung tạo Problem sai status cho lỗi ngoài decode 400

- Vị trí: `solutions/ch13-code/src/main/scala/vn/scalaeng/solutions/ch13/Ch13Solutions.scala:46`; lời giải tương ứng `solutions/ch13-solutions.md:74`; gợi ý trong bài `05-backend/ch13-choosing-a-framework.md:530`.
- `defaultHandlers` thay renderer của **exception, decode failure và reject**, trong khi callback luôn tạo `Problem(400, "Bad Request", msg)`. Vì vậy các HTTP status khác 400 có body sai. Đã chạy đúng `TapirProblemOnDecodeFailure.routes` với `PUT /api/v1/products`, nhận output thật:

  ```text
  PROBE reject: HTTP=405; BODY={"status":400,"title":"Bad Request","detail":"Method Not Allowed","errors":[]}
  ```

- Lỗi này quan trọng vì lời giải được giới thiệu là cách dùng tiếp cho toàn bộ API chương 14; client đọc `status` trong body sẽ hiểu sai lỗi. Test hiện tại chỉ gửi JSON hỏng nên không phát hiện.
- Đề xuất: nếu bài chỉ sửa decode failure, cấu hình riêng `DefaultDecodeFailureHandler` và để exception/reject giữ mặc định; nếu muốn mọi lỗi đều là Problem, cấu hình từng handler với status thực tế, đồng bộ HTTP status/body/title. Thêm ca 405 và 500, ngoài ca decode 400.
- Nguồn chính thức, kiểm tra 17/10/2026: [Tapir — Customising how error messages are rendered](https://tapir.softwaremill.com/en/latest/server/errors.html#customising-how-error-messages-are-rendered).

### M2 — Mô tả quá mức bảo đảm của kiểu Tapir và OpenAPI

- Vị trí: `05-backend/ch13-choosing-a-framework.md:159`, liên quan dòng 128.
- Khẳng định status/lỗi/body sai sẽ thành lỗi biên dịch khiến người mới hiểu compiler bảo đảm cả status và nghiệp vụ. Thực tế `Problem.status` là `Int`; điều kiện trong `oneOfVariantValueMatcher` được kiểm tra lúc chạy. Reviewer gắn logic trả `Left(Problem(409, ...))` vào chính `TapirApi.getProduct` (endpoint chỉ khai báo 404): **biên dịch được**, khi gọi nhận:

  ```text
  PROBE well-typed mismatched error: HTTP=500; BODY=Internal server error
  ```

- Tương tự, sinh OpenAPI từ endpoint giúp giảm lệch ở phần khai báo, không bảo đảm tài liệu khớp mọi validation nghiệp vụ/interceptor. Ví dụ lab validate giá/SKU trong `Validation.validate`, schema tự derive chưa mô tả những ràng buộc đó; handler lời giải cũng không tự cập nhật `TapirApi.openApiYaml`.
- Đề xuất: viết rõ compiler kiểm tra **kiểu input/error/output**, còn giá trị status, logic validation, matcher và cấu hình handler vẫn cần test; thay kết luận tuyệt đối về OpenAPI bằng phạm vi bảo đảm thực tế. Có thể minh họa bằng ca sai status trên hoặc ADT lỗi chuyên biệt hơn.

## MINOR

### m1 — Phiên bản thư viện test trong lời giải/handoff không khớp project

- Vị trí: `solutions/ch13-solutions.md:4`, `.team/handoffs/T02-ch13-framework.md:17`.
- Lời giải gắn kết quả 5/5 với `munit-cats-effect 2.2.1`, nhưng cả hai `project.scala` Typelevel/lời giải hiện dùng **2.2.0**, đúng AGENTS.md. Handoff chưa phân biệt rõ bản mới nhất tra được và bản thực dùng kiểm chứng.
- Đề xuất: sửa lời giải thành 2.2.0; trong handoff tách bản đã tra và bản chốt thực chạy. Không nâng dependency theo hint của Scala CLI.

### m2 — Transcript curl thiếu dữ liệu và trạng thái khởi đầu để tái lập

- Vị trí: `05-backend/ch13-choosing-a-framework.md:347` và dòng 356.
- `p.json` chưa được cung cấp nội dung/lệnh tạo, request đầu đã trả id=2, request tiếp lại giả định KB-01 tồn tại với id=1. Server mới chạy tạo store rỗng, nên người mới làm theo chưa tái lập được transcript.
- Đề xuất: thêm bước tạo KB-01, ghi nội dung UTF-8 của `p.json` (MS-01), rồi chạy các request theo thứ tự; ghi rõ `...` là rút gọn cùng URL/header. Có thể giữ transcript thật hiện có nếu bổ sung trạng thái ban đầu.

## Kiểm chứng thực chạy

Môi trường: Windows, Scala CLI 1.9.1; directives Scala **3.9.0**, JDK **21**. Đặt:

```powershell
$env:COURSIER_CACHE = "$PWD/.cache/coursier"
$env:SCALA_CLI_HOME = "$PWD/.cache/scala-cli"
scala-cli.bat test 05-backend/framework-lab/typelevel --server=false --workspace .cache/review-t02-typelevel
scala-cli.bat test 05-backend/framework-lab/zio-http --server=false --workspace .cache/review-t02-zio
scala-cli.bat test solutions/ch13-code --server=false --workspace .cache/review-t02-solutions
```

| Phạm vi | Kết quả thật |
|---|---|
| Http4sContractSuite | 5 total, 0 failed, 0 ignored |
| OpenApiSuite | 3 total, 0 failed, 0 ignored |
| TapirContractSuite | 5 total, 0 failed, 0 ignored |
| ZioApiSpec | 5 passed, 0 failed, 0 ignored |
| Ch13SolutionsSuite | 5 total, 0 failed, 0 ignored; gồm socket HTTP thật |

**Tổng 23 pass, 0 fail, 0 skip**, exit code 0 của ba lệnh. Không thấy compiler warning; có hint cập nhật dependency và SLF4J NOP warning đúng như bài đã giải thích. Lần Typelevel đầu chạy qua pipeline PowerShell có exit status của shell không đáng tin do stderr; đã chạy lại bằng redirect trực tiếp và xác nhận 13/13, exit code 0.

Đã chạy `PrintOpenApi` bằng `--server=false`: phần GET, response 400/404, schema và `errors` không required khớp đoạn trích trong bài (bỏ qua encoding console/thời gian). Contract test kiểm chứng body Product/Problem chính; transcript curl còn cần bổ sung các bước ở m2. Không chạy lại từng bản sao cố ý làm hỏng import để tái tạo mọi diagnostic ở mục 10; không có bằng chứng để kết luận các diagnostic đó sai.

Probe bổ sung đặt trong `.cache/review-t02-probe.scala`, chạy `scala-cli.bat run ... --server=false --main-class ReviewT02Probe`; log `.cache/review-t02-probe.log`. Các cảnh báo chaining directive của probe là do cách reviewer tái sử dụng nguồn, không tính cho lab gốc.

## Template, sư phạm và bài tập

- Đủ **17/17 mục**, không có mục rỗng. Tiếng Việt có dấu, domain nối tiếp chương 12, giải thích route/endpoint/runtime và lựa chọn thay thế rõ; bảng tách nhận định khỏi bằng chứng hữu ích cho người mới.
- Sáu bài tập có lời giải tương ứng; bài 2–5 có 5 test thật. Bài 1 và mini project là câu hỏi mở, tiêu chí đã có. Cần sửa lời giải bài 4 như M1.
- Code dùng đúng package, Scala/JDK và options theo chuẩn; ZIO tách project riêng, dùng zio-test thay munit để minh họa hệ ZIO.

## Đối chiếu nguồn ngoài

Đã đọc [Play 3.0 Highlights](https://www.playframework.com/documentation/3.0.x/Highlights30): xác nhận Scala 3, chuyển sang Pekko, đổi groupId. Đã tải trực tiếp Maven Central bằng Python urllib sau khi web tool/PowerShell không đọc được XML:

- [http4s metadata](https://repo1.maven.org/maven2/org/http4s/http4s-ember-server_3/maven-metadata.xml): bản không có hậu tố prerelease cuối là 0.23.38; milestone 1.0.0-M49.
- [Tapir metadata](https://repo1.maven.org/maven2/com/softwaremill/sttp/tapir/tapir-core_3/maven-metadata.xml): 1.13.33.
- [Play metadata](https://repo1.maven.org/maven2/org/playframework/play_3/maven-metadata.xml): stable 3.0.12, milestone 3.1.0-M9.
- [ZIO HTTP metadata](https://repo1.maven.org/maven2/dev/zio/zio-http_3/maven-metadata.xml): 3.11.6.
- [Doobie metadata](https://repo1.maven.org/maven2/org/typelevel/doobie-core_3/maven-metadata.xml): 1.0.0-RC13, chưa có 1.0.0 final.
- [fs2-kafka metadata](https://repo1.maven.org/maven2/org/typelevel/fs2-kafka_3/maven-metadata.xml): 4.1.3.

Các kết luận phiên bản chính trong bài/lời giải phù hợp nguồn đã kiểm tra ngày 17/10/2026. Play không có lab, đúng giới hạn đã khai báo; reviewer không suy ra đã kiểm chứng runtime Play.
