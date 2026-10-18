# Handoff T01 — Ch16B: Cats Effect 3

- **Tác giả:** Codex · **Reviewer được giao:** Claude Code.
- **Ngày kiểm chứng:** vòng 1 ngày 16/10/2026; vòng 2 hoàn tất ngày 17/10/2026 (Asia/Bangkok).
- **Trạng thái:** Claude đã review chéo ngày 16/10/2026; Codex đã sửa cả 4 MAJOR và 5 MINOR ở **Vòng 2** bên dưới. Chờ lead kiểm tra vòng sửa và kiểm chứng cuối; không cập nhật file dùng chung, không commit.
- **Kết quả:** lab **22/22**, lời giải **12/12** test munit thật pass; **0 fail, 0 ignored, 0 cảnh báo compiler**. Bật `-deprecation -feature -Wunused:all -Werror` ở cả hai build Scala CLI.
- **sbt:** Claude đã chạy `sbt.bat -batch test` ngoài sandbox, **22/22 pass**, sbt 1.12.11, theo `.team/reviews/T01-ch16b-cats-effect-review.md`. Codex không chạy lại sbt trong vòng 2. Log sbt thất bại ở vòng 1 được giữ làm lịch sử, không đại diện cho trạng thái build hiện tại.

> Mục 1–6 dưới đây lưu báo cáo **vòng 1** tại thời điểm bàn giao ban đầu. Những ghi chú “chờ review” / “sbt chưa kiểm chứng” trong phần lịch sử đã được thay thế bởi kết quả reviewer ở trên và mục **Vòng 2** ở cuối. Các log test/demo hiện trỏ tới lần chạy lại vòng 2.

## 1. File đã tạo

Không sửa file sẵn có của tác giả khác. Các file dưới đều nằm trong phạm vi brief:

### Bài học và lab

- `07-concurrency/ch16b-cats-effect.md` — **30.669 byte UTF-8**, đủ 17 mục template; có 12 tiểu mục lý thuyết, 6 bài tập, mini project, nguồn và checklist.
- `07-concurrency/effects-lab/.gitignore` — bỏ qua `.cache/` riêng của task.
- `07-concurrency/effects-lab/project.scala` — version/dependency/cờ compiler Scala CLI.
- `07-concurrency/effects-lab/build.sbt` — build sbt tương đương, một module.
- `07-concurrency/effects-lab/project/build.properties` — sbt 1.12.11.
- `07-concurrency/effects-lab/src/main/scala/vn/scalaeng/effects/Effects.scala` — Retry, model, FakePool/Resource/Semaphore và Checkout.
- `07-concurrency/effects-lab/src/main/scala/vn/scalaeng/effects/Comparisons.scala` — harness Future/IO: eager, cancel, timeout, retry, resource, starvation, interruption.
- `07-concurrency/effects-lab/src/main/scala/vn/scalaeng/effects/EffectsDemo.scala` — SimpleExample, EffectsDemo, ErrorDemo.
- `07-concurrency/effects-lab/src/test/scala/vn/scalaeng/effects/EffectsSuite.scala` — 22 test.

### Lời giải

- `solutions/ch16b-solutions.md` — giải thích đủ 6 bài, mini project, đáp án 8 câu kiểm tra, lệnh test và giới hạn.
- `solutions/effects-code/project.scala` — tái dùng **Effects.scala** của lab qua `using file`, cùng version/cờ compiler; không kéo test lab vào.
- `solutions/effects-code/src/main/scala/vn/scalaeng/effectsolutions/Exercises.scala` — replay, fallback chọn lọc, Resource lồng, boundedTraverse, retryRead, batch.
- `solutions/effects-code/src/test/scala/vn/scalaeng/effectsolutions/ExercisesSuite.scala` — 12 test.

### Bằng chứng và tái lập

- `07-concurrency/effects-lab/verification/verify_versions.py` — GET metadata/POM/source chính thức; không thuộc unit test vì cần mạng.
- `07-concurrency/effects-lab/verification/versions.json` — thời điểm UTC, URL, SHA-256, latest/release, dependency trong 5 POM, sự tồn tại executeEmbed trong source.
- `07-concurrency/effects-lab/verification/run_checks.py` — chạy lệnh thật, lưu stdout/stderr, bỏ ANSI, ghi exit code; mặc định không chạy sbt đang bị chặn.
- `07-concurrency/effects-lab/verification/lab-test.log`.
- `07-concurrency/effects-lab/verification/solutions-test.log`.
- `07-concurrency/effects-lab/verification/simple.log`.
- `07-concurrency/effects-lab/verification/demo.log`.
- `07-concurrency/effects-lab/verification/errors.log`.
- `07-concurrency/effects-lab/verification/sbt-test.log` — **lần chạy thất bại**, giữ nguyên để không giấu giới hạn.
- `07-concurrency/effects-lab/verification/content-check.log` — kiểm tra cấu trúc, link, đối chiếu output/lỗi với log, UTF-8/file không rỗng/whitespace.
- `.team/handoffs/T01-ch16b-cats-effect.md` — báo cáo này.

Cache tải dependency, sbt boot cache và script kiểm tra tạm nằm trong `effects-lab/.cache/`, đã ignore; không đưa cache vào commit. `.scala-build/`/`.bsp/` dùng quy tắc ignore sẵn có của repo.

## 2. Lệnh đã chạy và output tóm tắt thật

Môi trường: Windows, Scala CLI **1.9.1**, Scala **3.9.0**, JDK **21**. `scala-cli ... --command` xác nhận dùng Temurin **21.0.12.1+1** trong Coursier JVM cache; `java -version` trên PATH riêng là Oracle 21.0.7, không phải JVM Scala CLI chọn theo directive. Không dùng Bloop.

Sandbox không cho Coursier ghi cache mặc định. Đã giải quyết bằng biến môi trường trong từng tiến trình/phiên:

```powershell
$env:COURSIER_CACHE = Join-Path $PWD '07-concurrency/effects-lab/.cache/coursier'
scala-cli.bat test 07-concurrency/effects-lab --server=false
scala-cli.bat test solutions/effects-code --server=false
```

Output lần cuối, trích nguyên dòng từ log, không tạo số liệu giả:

```text
Test run vn.scalaeng.effects.EffectsSuite started
vn.scalaeng.effects.EffectsSuite: finished 0.908s
Test run vn.scalaeng.effects.EffectsSuite finished: 0 failed, 0 ignored, 22 total 0.914s

Test run vn.scalaeng.effectsolutions.ExercisesSuite started
vn.scalaeng.effectsolutions.ExercisesSuite: finished 0.378s
Test run vn.scalaeng.effectsolutions.ExercisesSuite finished: 0 failed, 0 ignored, 12 total 0.385s
```

Hai lệnh exit **0**. Scala CLI in **hint** munit 1.3.6/munit-cats-effect 2.2.1 mới hơn; không phải cảnh báo compiler. Không tắt cảnh báo; `-Werror` vẫn bật và source không có annotation suppress cảnh báo.

Đã chạy cả ba executable, exit **0**:

```powershell
scala-cli.bat run 07-concurrency/effects-lab --server=false --main-class vn.scalaeng.effects.SimpleExample
scala-cli.bat run 07-concurrency/effects-lab --server=false --main-class vn.scalaeng.effects.EffectsDemo
scala-cli.bat run 07-concurrency/effects-lab --server=false --main-class vn.scalaeng.effects.ErrorDemo
```

Output ứng dụng, trích từ `simple.log`/`demo.log`:

```text
first=1 second=2
eager: Future before=1 after=1; IO before=0 after=2
cancel: canceled=true writes=0 finalizer=true
timeout: Future error=true before=0 after=1; IO error=true writes=0 finalizer=true
retry: Future cached=1 fresh=3; IO=3
resource: Future closedBeforeRead=true
open connection -> open statement -> close statement -> close connection
java.lang.IllegalStateException: query failed
starvation: Future timedOut=true; blocking+compute=49; fiber join=42
interruptible: interrupted=true
quote: price=120000 stock=3 discount=0 payable=120000 requests=1
pool: opened=3 closed=3 last=close pool
```

Output lỗi runtime có chủ đích, trích `errors.log`:

```text
java.lang.IllegalArgumentException: unknown sku X
java.util.concurrent.TimeoutException: 20 milliseconds
java.lang.IllegalArgumentException: pool size must be positive
```

Demo bắt exception qua attempt để in toString rồi thoát 0; không ngụy tạo stacktrace compiler/runtime. Mục 9 và 10 bài học đã đối chiếu từng dòng với log. `blocking+compute=49` là tổng giá trị 42+7, không phải milliseconds.

Tái lập kiểm chứng và nguồn:

```powershell
python 07-concurrency/effects-lab/verification/run_checks.py
python 07-concurrency/effects-lab/verification/verify_versions.py
```

`content-check.log` xác nhận **17/17 mục**, link nội bộ tồn tại, output/lỗi khớp log, 22+12 test/0 compiler warnings, các artifact không rỗng, đọc được UTF-8 và không trailing whitespace. Check script tạm ở `.cache/check_content.py` không phải dependency của lab.

## 3. Phiên bản và nguồn đã kiểm chứng — 16/10/2026

| Thành phần | Chốt / bằng chứng |
|---|---|
| Cats Effect | **3.7.1**, stable 3.7.x mới nhất ở [metadata `_3`](https://repo.maven.apache.org/maven2/org/typelevel/cats-effect_3/maven-metadata.xml); [POM](https://repo.maven.apache.org/maven2/org/typelevel/cats-effect_3/3.7.1/cats-effect_3-3.7.1.pom), compile/run thật Scala 3.9.0 |
| munit | **1.2.4** theo quy ước repo, test với artifact thật |
| munit-cats-effect | **2.2.0**: [POM `_3`](https://repo.maven.apache.org/maven2/org/typelevel/munit-cats-effect_3/2.2.0/munit-cats-effect_3-2.2.0.pom) dùng munit 1.2.4 và CE 3.7.0; đã chạy với CE patch 3.7.1 |
| Bản munit-cats-effect mới hơn | **2.2.1** ở [metadata](https://repo.maven.apache.org/maven2/org/typelevel/munit-cats-effect_3/maven-metadata.xml), [POM](https://repo.maven.apache.org/maven2/org/typelevel/munit-cats-effect_3/2.2.1/munit-cats-effect_3-2.2.1.pom) dùng munit **1.3.6**, nên không chọn để giữ chuẩn repo |
| cats-effect-testkit | **3.7.1**, có [POM `_3`](https://repo.maven.apache.org/maven2/org/typelevel/cats-effect-testkit_3/3.7.1/cats-effect-testkit_3-3.7.1.pom); [source TestControl](https://github.com/typelevel/cats-effect/blob/v3.7.1/testkit/shared/src/main/scala/cats/effect/testkit/TestControl.scala) có executeEmbed; test thời gian ảo chạy thật |
| ZIO chỉ so sánh | **2.1.27**, [metadata `_3`](https://repo.maven.apache.org/maven2/dev/zio/zio_3/maven-metadata.xml), [POM](https://repo.maven.apache.org/maven2/dev/zio/zio_3/2.1.27/zio_3-2.1.27.pom), [source ZIO[R,E,A]](https://github.com/zio/zio/blob/v2.1.27/core/shared/src/main/scala/zio/ZIO.scala); không chạy lab ZIO |
| Scala / JDK / sbt | Pin **3.9.0 / 21 / 1.12.11** theo brief. Scala/JDK đã chạy; sbt chưa qua launcher vì sandbox |

Nguồn semantics: [Resource](https://github.com/typelevel/cats-effect/blob/v3.7.1/docs/std/resource.md), [MonadCancel](https://github.com/typelevel/cats-effect/blob/v3.7.1/docs/typeclasses/monadcancel.md), [thread model](https://github.com/typelevel/cats-effect/blob/v3.7.1/docs/thread-model.md), [CatsEffectSuite](https://typelevel.org/munit-cats-effect/). Thread model có nội dung lịch sử CE2; bài chỉ dùng API CE3.

## 4. Phạm vi hành vi đã có test

- Sáu giới hạn Future trong brief đều có đối chiếu chạy thật. Future timeout dùng cổng Promise, sau timeout mới mở cổng/chờ Future gốc để chứng minh còn ghi; IO dùng Deferred và kiểm tra hủy trước khi mở cổng.
- IO lazy/replay, IO.pure tính đối số sớm; attempt/handleErrorWith/raiseError.
- Fiber start/join/cancel, onCancel, uncancelable/poll, finalizer khi lỗi/hủy; blocking pool và Thread.interrupt trên API hỗ trợ.
- Resource đóng ngược, pool trả connection/permit sau lỗi; Ref đủ 1.000 cập nhật; Deferred nhiều reader/complete chỉ thắng một lần.
- parMapN ba service dùng handshake bắt buộc đồng thời; giá lỗi hủy hai sibling đã bắt đầu và đóng đủ ba connection. Giá timeout giữ lỗi; stock/promotion timeout fallback có cleanup.
- Backoff có trần, zero retries, input sai và ranh giới cap lẻ nanosecond; retry chọn lỗi/giữ lỗi cuối/hủy trong sleep; TestControl đo 100+200 ms ảo.
- Sáu lời giải: replay, fallback chọn lọc, Resource lỗi/hủy, boundedTraverse peak/order/error cleanup, retryRead và batch thành công/lỗi/rỗng.

Hai điểm diễn đạt được làm rõ để tránh tiếp tục hiểu nhầm từ Ch16A: Future có thể chia sẻ kết quả nhưng không replay thân; retry trong IO vẫn phải có policy do ứng dụng quyết định. Timeout/hủy có hợp tác, không phải hard deadline/rollback side effect đã hoàn thành.

## 5. Chưa kiểm chứng / giới hạn

1. **sbt.bat test chưa chạy qua launcher**, không có số test sbt pass để báo. Đã chuyển Coursier, boot/global/ivy cache vào `.cache/`, tắt server autostart/supershell, thử thêm cấu hình boot; vẫn lỗi trước khi đọc build:

   ```text
   java.nio.file.AccessDeniedException: C:\Users\VuTT\zero_to_hero_scala\07-concurrency\effects-lab
       at java.base/sun.nio.fs.WindowsPath.toRealPath(WindowsPath.java:947)
       at sbt.internal.BootServerSocket.<init>(BootServerSocket.java:287)
   ```

   `sbt-test.log` giữ output đầy đủ, exit **1**. Tái lập: `python 07-concurrency/effects-lab/verification/run_checks.py sbt-test`. Đây là giới hạn sandbox Windows; tiêu chí hai lệnh Scala CLI của brief đã pass. Lead cần chạy sbt 1.12.11 ngoài môi trường bị chặn để kiểm chứng build.sbt.
2. **Chưa review chéo**: chỉ Claude Code mới viết review/kiểm chứng cuối và cập nhật trạng thái chung. Không tự tuyên bố được reviewer chấp thuận.
3. **ZIO**: chỉ xác nhận artifact Scala 3 và khái niệm qua nguồn chính thức, không compile/run với Scala 3.9.0. Đúng phạm vi brief không có lab ZIO.
4. **Dịch vụ/pool giả**: không có HTTP/JDBC/DB/Kafka/Docker; không tuyên bố kiểm chứng driver interruption, throughput, semaphore fairness, giao dịch hay exactly-once. Không có integration test cần skip.
5. Retry lab chưa thêm jitter, deadline tổng hoặc retry ghi; đã nêu các giới hạn và yêu cầu idempotency. TestControl không giả lập OS/thread blocking thật; các thí nghiệm interruption/starvation dùng runtime thật.

## 6. Đề xuất cho lead áp dụng vào file chung

### PROGRESS.md

- Thêm Ch16B: bài học 17 mục, code, 6 lời giải/mini project, **22+12 test munit thật**, Scala CLI compile/run với `-Werror`, chờ Claude review.
- Chỉ đánh dấu hoàn tất sau review chéo, xử lý BLOCKER/MAJOR và lead chạy lại test; ghi riêng sbt chưa kiểm chứng nếu sandbox của lead cũng chặn.
- Không áp kết quả này cho test munit của Ch16A hoặc chương khác: task không chạy lại/chỉnh các lab đó.

### glossary/glossary.md

Đề xuất thêm effect description, referential transparency, runtime, fiber, cooperative cancellation, finalizer, masking/poll, Resource scope, Ref/CAS update, Deferred one-shot signal, Semaphore permit, virtual time/TestControl; dùng định nghĩa ngắn ở mục 4/3 của bài. Phân biệt “hủy fiber” và “ngắt thread”, “replay mô tả” và “cache kết quả”.

### 00-roadmap/04-compatibility-matrix.md

- Pin Cats Effect **3.7.1**, `_3`, compile/run thật Scala 3.9.0 + JDK 21 ngày 16/10/2026.
- Bổ sung munit-cats-effect **2.2.0** + munit **1.2.4**, CE testkit **3.7.1**, TestControl.executeEmbed đã chạy; 2.2.1 dùng munit 1.3.6 nên chưa nâng theo chuẩn repo.
- ZIO **2.1.27** có `_3`; trạng thái chỉ “artifact Scala 3 đã xác nhận, chưa compile/run Scala 3.9.0”, không chuyển thành nhãn chạy thành công.
- Có thể làm rõ bảng Ch16A về Future replay và retry policy khi lead chỉnh chương đó; task này không sửa file ngoài brief.

Các thay đổi khác của lead/agent khác đang tồn tại trong working tree; đã để nguyên. Không git add/commit/reset/checkout, không chỉnh README/PROGRESS/roadmap/glossary hay file dùng chung.

## Vòng 2 — Sửa sau review của Claude Code

**Ngày:** 17/10/2026 (Asia/Bangkok; một số log compiler được chạy trước nửa đêm 16/10, timestamp log dùng UTC).
**Review gốc:** [T01-ch16b-cats-effect-review.md](../reviews/T01-ch16b-cats-effect-review.md).
Đã xử lý đủ **MAJOR 1–4, MINOR 5–9**; không thay đổi hành vi nghiệp vụ hay dependency của lab/lời giải.

### Đối chiếu từng mục review

1. **MAJOR 1:** bổ sung **12 đoạn Scala** vào đủ các mục **3.2–3.11**, mỗi đoạn 5–15 dòng, trích từ lab hoặc lời giải đã compile/test và chú thích các dòng quan trọng. Có IO.delay/replay, IOApp/for, blocking, error handling, Resource.make/use, start/cancel/join, parMapN/timeoutTo, Ref.update, Deferred.get/complete, semaphore.permit.use/parTraverse, Retry.apply và CatsEffectSuite/TestControl. Các đoạn ngữ cảnh trong for hoặc CatsEffectSuite đều được ghi rõ vị trí trích. Script check_content đối chiếu từng dòng code (bỏ chú thích) với source Scala đang dùng.
2. **MAJOR 2:** viết lại mục 3.2–3.11 và mục 8 thành các đoạn giải thích đầy đủ, mỗi ý thành câu có chủ ngữ/vị ngữ; giữ các giới hạn kỹ thuật về cancellation có hợp tác, chờ cleanup, scope tài nguyên, thứ tự kết quả, CAS, retry và thời gian ảo. Mục 3.12 cũng được chuyển từ ghi chú ngắn sang đoạn giải thích. Bài hiện có 53885 byte UTF-8; vẫn đủ 17 mục template.
3. **MAJOR 3:** mục 10.2–10.4 trích **chẩn đoán compiler thật**, liên kết log đầy đủ và lệnh tái lập. Script compile_errors.py sao chép project.scala vào `.cache/compiler-examples/`, biên dịch bản đúng trước rồi sửa bản sao. Thiếu syntax parTraverse cho E008; thiếu IORuntime cho E172; bỏ rơi IO được `-Wnonunit-statement` phát hiện bằng E176. Có đối chứng `-Wvalue-discard`: cùng source bỏ IO **exit 0, không cảnh báo**, còn `-Wnonunit-statement` + `-Werror` **exit 1**. Không suy đoán thông báo và không đặt source lỗi vào src của lab.
4. **MAJOR 4:** mục 7 lấy thao tác vào effects-lab rồi `scala-cli.bat test . --server=false` và `sbt.bat test` làm lệnh chính. COURSIER_CACHE chỉ còn là ghi chú có điều kiện khi môi trường không ghi được cache mặc định; lời giải cũng dùng ghi chú có điều kiện tương tự.
5. **MINOR 5:** đưa tiêu đề chương lên dòng 1, thêm ngay bên dưới khối phiên bản / Code / Trạng thái kiểm chứng. Ghi 22/22 (sbt + Scala CLI) + 12/12 lời giải, munit thật, đã review chéo; phân biệt rõ sbt do Claude chạy và vòng sửa vẫn chờ lead kiểm tra.
6. **MINOR 6:** thêm “Vị trí trong lộ trình”: học sau 16A, trước Ch13; Ch13–15 dùng IO/Resource/Ref. Có link tới chương liên quan.
7. **MINOR 7:** bổ sung đủ thuật ngữ IO, Resource, Fiber, Outcome, Ref, Deferred, Semaphore, parMapN, TestControl, IOApp vào mục 4 (kèm parTraverse).
8. **MINOR 8:** mục 7 ghi build.sbt **đã được Claude chạy 22/22 pass** với sbt 1.12.11 ngoài sandbox và liên kết review, thay cho “chưa chạy qua do sandbox”. Handoff hiện tại cũng cập nhật trạng thái; giữ báo cáo vòng 1 như lịch sử có ghi chú rõ ràng.
9. **MINOR 9:** mục 16 thêm cổng tài liệu Cats Effect cùng link trang đọc về IO, Resource, Concepts (concurrency/cancellation) và Test Runtime (Testing/TestControl). Đã mở kiểm tra các trang nội dung chính thức ngày 16/10/2026; giữ nguồn POM/version đã chốt.

### File đã sửa / tạo ở vòng 2

- `07-concurrency/ch16b-cats-effect.md`: phần giảng giải, code trích, chẩn đoán compiler, lệnh chạy, thuật ngữ, lộ trình, nguồn và trạng thái kiểm chứng.
- `solutions/ch16b-solutions.md`: trạng thái review/test vòng 2 và ghi chú cache theo điều kiện môi trường.
- `07-concurrency/effects-lab/verification/compile_errors.py` (**mới**): tạo bản sao tạm, compile bản đúng/bản lỗi và so sánh hai cờ cảnh báo.
- `07-concurrency/effects-lab/verification/compiler-*.log` (**7 log mới**): ba baseline pass, ba ca lỗi chủ ý và một đối chứng -Wvalue-discard pass.
- `07-concurrency/effects-lab/verification/check_content.py` (**mới**): kiểm tra 17 mục, link nội bộ, độ dài/code trích, output, chẩn đoán và log test; không thay thế munit.
- `07-concurrency/effects-lab/verification/{lab-test,solutions-test,simple,demo,errors,content-check}.log`: cập nhật bằng lần chạy lại thật.
- `.team/handoffs/T01-ch16b-cats-effect.md`: cập nhật trạng thái hiện tại và báo cáo Vòng 2 này.
- `.cache/` chứa source thử nghiệm và script biên tập tạm, đã ignore; không đưa cache vào bàn giao. **Không sửa source/build của lab hoặc solutions/effects-code vì review không yêu cầu thay đổi hành vi.**

### Lệnh kiểm chứng và kết quả thật

Trong sandbox, cache vẫn được script đặt tại `07-concurrency/effects-lab/.cache/coursier` theo AGENTS.md. Từ repo root:

```powershell
python 07-concurrency/effects-lab/verification/compile_errors.py
python 07-concurrency/effects-lab/verification/run_checks.py lab-test solutions-test simple demo errors
python 07-concurrency/effects-lab/verification/check_content.py
```

Script run_checks thực thi nguyên các lệnh Scala CLI sau với `--server=false`:

```powershell
scala-cli.bat test 07-concurrency/effects-lab --server=false
scala-cli.bat test solutions/effects-code --server=false
scala-cli.bat run 07-concurrency/effects-lab --server=false --main-class vn.scalaeng.effects.SimpleExample
scala-cli.bat run 07-concurrency/effects-lab --server=false --main-class vn.scalaeng.effects.EffectsDemo
scala-cli.bat run 07-concurrency/effects-lab --server=false --main-class vn.scalaeng.effects.ErrorDemo
```

**5 lệnh exit 0**. Tóm tắt output test thật từ log vòng 2:

```text
Test run vn.scalaeng.effects.EffectsSuite started
vn.scalaeng.effects.EffectsSuite: finished 0.856s
Test run vn.scalaeng.effects.EffectsSuite finished: 0 failed, 0 ignored, 22 total 0.864s

Test run vn.scalaeng.effectsolutions.ExercisesSuite started
vn.scalaeng.effectsolutions.ExercisesSuite: finished 0.344s
Test run vn.scalaeng.effectsolutions.ExercisesSuite finished: 0 failed, 0 ignored, 12 total 0.35s
```

Lab **22 pass**, lời giải **12 pass**, **0 failed, 0 ignored, 0 cảnh báo compiler**, giữ `-deprecation -feature -Wunused:all -Werror`. Hint dependency mới của Scala CLI vẫn chỉ là hint; không nâng munit/munit-cats-effect. Ba executable chạy lại thành công, output ở mục 9 và lỗi runtime ở mục 10 khớp log mới.

Kết quả tái lập compiler (mỗi command đầy đủ ở đầu log):

| Ca | Cờ bổ sung / thay đổi | Exit thật |
|---|---|---|
| missing-syntax-baseline | Có cats.syntax.all.* | 0 |
| missing-syntax | Bỏ import, gọi List.parTraverse | 1, E008 |
| missing-runtime-baseline | Có cats.effect.unsafe.implicits.global | 0 |
| missing-runtime | Bỏ runtime import, gọi unsafeRunSync | 1, E172 |
| discarded-io-baseline | Ghép action *> IO.unit | 0 |
| discarded-io-value-discard | Bỏ IO, thêm -Wvalue-discard | 0, không cảnh báo |
| discarded-io-nonunit-statement | Cùng source bỏ IO, thêm -Wnonunit-statement | 1, E176 + -Werror |

Đây là **3 lỗi compile chủ ý**, không phải lỗi còn tồn tại trong lab. Source lỗi chỉ nằm trong `.cache/`; mọi bản baseline đều compile thành công trước khi tạo lỗi. Các khối trích compiler chỉ bỏ ANSI/dòng trống, không sửa nội dung thông báo.

### Phiên bản / nguồn / giới hạn còn lại

- Giữ nguyên Scala **3.9.0**, JDK **21**, Cats Effect/testkit **3.7.1**, munit **1.2.4**, munit-cats-effect **2.2.0**, sbt **1.12.11**. Không thêm dependency. Nguồn version đã kiểm chứng **16/10/2026** vẫn là metadata/POM chính thức ở mục 3 vòng 1 và `verification/versions.json`; lần này chạy lại cùng artifact.
- Link bổ sung đã mở kiểm tra: [IO](https://typelevel.org/cats-effect/docs/datatypes/io), [Resource](https://typelevel.org/cats-effect/docs/std/resource), [Concepts](https://typelevel.org/cats-effect/docs/concepts), [Test Runtime](https://typelevel.org/cats-effect/docs/core/test-runtime). Bài cũng liên kết cổng `/docs/`; các trang hướng dẫn có thể chứa cấu hình test cũ nên bài chỉ định rõ version lấy từ POM.
- Codex không chạy lại sbt vòng 2; kết quả **22/22 sbt** là kiểm chứng thật do **Claude Code** ghi trong review ngày 16/10/2026. Không thay thế log sbt sandbox lỗi vòng 1 bằng output giả của reviewer.
- Chưa có lượt reviewer xác nhận các sửa đổi vòng 2. Lead cần kiểm tra lại trước khi cập nhật tiến độ chung/commit. Các giới hạn ZIO chỉ kiểm chứng artifact, dịch vụ/pool giả và không có integration DB/HTTP vẫn giữ nguyên.
- Đề xuất cho lead: PROGRESS ghi đã xử lý 4 MAJOR + 5 MINOR, Scala CLI 22+12 pass sau sửa; compatibility matrix giữ version đã chốt; glossary có thể lấy thêm 10 định nghĩa mới ở mục 4. Không tự sửa các file chung.
- Không đụng `05-backend/order-service` (T03), không sửa bài/code của agent khác, không git add/commit/reset/checkout.
