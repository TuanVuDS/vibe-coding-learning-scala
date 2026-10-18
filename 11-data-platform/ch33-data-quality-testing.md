# Chương 33 — Data Quality và Testing

> **Phiên bản:** Scala **3.9.0** · JDK 21 · circe-parser 0.14.17 · munit 1.2.4 · munit-scalacheck 1.2.0 (ScalaCheck 1.19.0) · sbt 1.12.11
> **Code:** [`quality-lab/`](quality-lab/) — pipeline đơn hàng của Chương 24 viết lại bằng Scala thuần, cùng **sáu tầng kiểm thử**: unit → check trên tập →
> data contract + tương thích schema → golden dataset + regression → drift → end-to-end; property test cho dữ liệu trễ, trùng, chạy lại, backfill
> **Trạng thái kiểm chứng:** Phiên bản tra `repo1.maven.org` ngày 17/10/2026. **18/18 test pass** (`scala-cli test .` và `sbt test`, 0 cảnh báo; sau review chéo), lời giải 7/7.
> Golden được **tính tay trước khi chạy** và khớp 100% ở lần chạy đầu. Mọi output, thông báo lỗi và phản ví dụ trong bài là output thật (Windows 11 + JDK 21).

---

## Vị trí trong lộ trình

Chương 10 dạy kỹ thuật test (munit, property, fixture); Chương 19–20 tách bản ghi lỗi và viết quality gate; Chương 24 đưa hợp đồng đối soát
"mọi dòng vào đúng một ngăn" vào pipeline Spark; Chương 30 bàn về event trùng và replay. Chương này **gom lại thành một chiến lược kiểm thử dữ liệu**:
test cái gì, ở tầng nào, với dữ liệu nào, và làm sao biết output **thay đổi ngoài mong đợi** khi không có đáp án để so. Chương 31 đặt các check này vào kiến
trúc (raw/staging/serving, data contract giữa các team); **Project 5** dùng chúng làm cổng chất lượng trước khi publish.

## 1. Mục tiêu học tập

1. Phân biệt **test code** (hàm có đúng không) với **test dữ liệu** (dữ liệu hôm nay có đúng không) — và vì sao pipeline cần cả hai.
2. Viết **unit test cho transformation** gom lỗi, chuẩn hóa, không tràn số.
3. Viết **check trên tập**: not null, uniqueness, referential integrity, khoảng giá trị, số dòng, độ mới (freshness); chọn mức Error/Warn.
4. Viết **data contract** máy đọc được, kiểm tra bản ghi theo contract, và phân loại thay đổi schema **compatible / breaking**.
5. Xây **golden dataset** đáng tin (tính tay, bao phủ ca khó) và dùng nó làm **regression test**.
6. Viết **property test** cho các tính chất mà pipeline dữ liệu phải có: không phụ thuộc thứ tự đến, chạy lại an toàn, trùng không đổi kết quả, dữ liệu
   trễ không đè dữ liệu mới, xử lý tăng dần ≡ tính lại toàn bộ (backfill), bảo toàn số dòng.
7. Phát hiện **drift** trên dữ liệu thật bằng hồ sơ (profile) và ngưỡng.
8. Viết **end-to-end test** đi qua đúng đường đọc/ghi file.

## 2. Kiến thức tiên quyết

Chương 10 (munit, ScalaCheck, golden/snapshot test), Chương 20 (validation gom lỗi, check là dữ liệu, `BigInt` cho tổng), Chương 21 (schema evolution với Avro),
Chương 24 (dedup hai tầng, xung đột, hợp đồng đối soát, ba chế độ ghi), Chương 27/30 (event trễ, trùng, replay). Không cần Spark: lab chạy trên collection để
tập trung vào **cách kiểm thử**; mọi kỹ thuật áp dụng nguyên vẹn cho DataFrame (mục 3.8).

## 3. Lý thuyết

### 3.1. Hai câu hỏi khác nhau: code có đúng? dữ liệu có đúng?

**Vấn đề.** Một pipeline có 100% test xanh vẫn có thể ghi ra số sai: nguồn đổi định dạng ngày, bảng khách hàng bị nhân đôi một dòng, file của một lô không đến.
Test code chạy **một lần trong CI** trên dữ liệu **bạn tự chọn**; dữ liệu thật đến **mỗi ngày** và bạn **không** chọn nó.

**Vì sao.** Pipeline là hàm `output = f(code, dữ liệu)`. Test code giữ `dữ liệu` cố định và kiểm tra `code`; check dữ liệu giữ `code` cố định và kiểm tra `dữ liệu`.

| | Test code | Check dữ liệu |
|---|---|---|
| Chạy khi | CI, mỗi lần đổi code | Mỗi lần pipeline chạy, trong production |
| Dữ liệu | Mẫu nhỏ, tự viết, biết đáp án | Thật, lớn, không biết đáp án |
| Thất bại nghĩa là | Code sai → không merge | Dữ liệu sai → không publish / cảnh báo |
| Công cụ trong lab | munit, ScalaCheck, golden | `Check`, `Contract`, `Drift` |

Lab có sáu tầng, từ rẻ và hẹp tới đắt và rộng:

```
 unit test (hàm)  →  check trên tập  →  contract  →  golden/regression  →  drift  →  end-to-end
   ms, rất nhiều       mỗi lần chạy       biên team       mẫu tính tay        dữ liệu thật    ít, chậm
```

**Lỗi thiết kế thường gặp:** chỉ có một trong hai loại. Chỉ test code → số sai đi thẳng tới dashboard. Chỉ check dữ liệu → mỗi lần sửa code là đánh cược.

### 3.2. Unit test cho transformation

Test hàm thuần như mọi hàm khác (Chương 10), nhưng chọn ca theo **cách dữ liệu hỏng thật sự**: viết hoa/thường, khoảng trắng, số âm, số 0, chuỗi không phải số,
timestamp sai, giá trị lớn gần biên. Hai điều riêng của dữ liệu:

- **Gom mọi lỗi** của một dòng (Chương 20), không dừng ở lỗi đầu — người sửa nguồn cần biết hết. Test so **cả danh sách lỗi**.
- **Biên số học**: `1000 × Long.MaxValue` tràn `Long`. Doanh thu cộng dồn dùng `BigInt` và có test riêng cho biên (Chương 15, 24 đều có lỗi tràn thật).

Unit test **không** bắt được: thứ tự đến khác, trùng giữa các lô, khóa dimension trùng — những lỗi chỉ xuất hiện trên **tập**.

### 3.3. Check trên tập: not null, unique, references, range, row count, freshness

**Vấn đề.** Nhiều quy tắc không kiểm tra được trên từng dòng: "khóa duy nhất" cần nhìn mọi dòng; "mọi `customer_id` có trong bảng khách" cần hai bảng;
"hôm nay có dữ liệu" cần biết ngày mới nhất.

**Cách hoạt động.** Trong lab, một check là **dữ liệu**: tên, mức độ, và hàm `Seq[A] => CheckResult` (số dòng kiểm, số vi phạm, vài mẫu để người trực đọc).

| Check | Bắt được | Ví dụ thật trong lab |
|---|---|---|
| `notNull` | trường bắt buộc rỗng sau join/parse | — |
| `unique` | trùng khóa → join nhân doanh thu, upsert sai | `(order_id, sku)` của fact |
| `references` | **toàn vẹn tham chiếu**: khóa ngoại không có ở dimension | `C9` không có trong `customers.csv` |
| `inRange` | giá trị phi lý (âm, quá lớn) | doanh thu dòng 1..10 tỷ |
| `rowCount` | **mất dữ liệu** — không quy tắc từng dòng nào bắt được | daily 1..1000 dòng |
| `freshness` | dữ liệu "đứng": nguồn ngừng gửi, job vẫn xanh | ngày mới nhất ≥ asOf − 2 |

**Error hay Warn?** Error chặn publish (Chương 24: write-audit-publish); Warn ghi log/cảnh báo nhưng vẫn publish. `C9` là Warn vì đơn hàng có thật, khách chỉ chưa
đồng bộ — chặn sẽ làm mất doanh thu khỏi báo cáo; ta gán `UNKNOWN` và theo dõi **tỷ lệ** UNKNOWN (bài 1, mục drift). Khóa trùng là Error: số liệu chắc chắn sai.

**Thay thế.** Thư viện chuyên dụng làm đúng việc này ở quy mô lớn: Deequ (Scala, chạy trên Spark), Great Expectations và Soda (Python), dbt data tests
(`unique`, `not_null`, `accepted_values`, `relationships` — viết bằng SQL). Lab tự viết để thấy cơ chế; trong dự án thật, chọn công cụ hợp với nơi dữ liệu nằm
(bảng SQL → dbt test; DataFrame Spark → Deequ hoặc check tự viết như lab).

**Lỗi thiết kế:** check mọi thứ ở mức Error → pipeline đỏ mỗi ngày, mọi người học cách bỏ qua nó. Check không có mẫu vi phạm → người trực phải tự truy vấn lại.

### 3.4. Data contract và tương thích schema

**Vấn đề.** Team checkout đổi `quantity` từ int sang long, thêm trạng thái `PARTIALLY_REFUNDED`. Pipeline của team dữ liệu vỡ lúc 2 giờ sáng. Không ai sai
về kỹ thuật — chỉ là không có **thỏa thuận** nào được kiểm tra.

**Data contract** là thỏa thuận giữa bên **sản xuất** và bên **tiêu thụ** về schema + ý nghĩa, ở dạng **máy kiểm tra được**: tên trường, kiểu, nullable, giá trị enum,
chủ sở hữu, phiên bản ([`contracts/orders.v1.json`](quality-lab/contracts/orders.v1.json)). Nó dùng ở hai chỗ:

1. **Producer conformance** tại biên ingest (`Contract.validate`, **nghiêm**): bản ghi nào không theo contract là lỗi của producer, ghi lại kèm lý do. Trường
   **thừa** cũng là vi phạm: producer đổi schema mà không đổi contract là chuyện cần biết ngay.
2. **Reader projection** (`Contract.read`): consumer viết theo contract có **đọc được** bản ghi không? Trường lạ bị **bỏ qua** (như Avro reader bỏ trường chỉ có ở
   writer); trường mình cần thì phải có và parse được. Hai API, hai câu hỏi khác nhau — đừng dùng một hàm cho cả hai.
3. **Kiểm tra thay đổi contract** (trong CI của producer): bản mới có làm vỡ consumer đang đọc theo bản cũ?

**Chọn hướng trước khi nói "tương thích".** Confluent Schema Registry đặt tên: **FORWARD** = reader **cũ** đọc dữ liệu của writer **mới**; **BACKWARD** = reader
**mới** đọc dữ liệu **cũ**; **FULL** = cả hai. Lab kiểm tra FORWARD — hướng của câu hỏi "producer nâng cấp trước, consumer chưa kịp deploy có vỡ không?". Cam
kết **kiểm được bằng test**: nếu mọi thay đổi là Compatible thì mọi bản ghi đạt `validate(mới, r)` đều đạt `read(cũ, r)`.

| Thay đổi (cũ → mới) | FORWARD | Vì sao |
|---|---|---|
| Thêm trường | Compatible | reader cũ bỏ qua trường lạ |
| Bỏ trường bắt buộc / đổi tên | Breaking | reader cũ cần trường đó |
| Bỏ trường nullable | Compatible | reader cũ vốn cho phép thiếu |
| Đổi kiểu, **kể cả int → long** | Breaking | writer mới được ghi `2147483648`; reader `int` cũ không parse được |
| long → int | Compatible | writer thu hẹp miền: mọi int đều là long hợp lệ |
| Bắt buộc → nullable | Breaking | reader cũ gặp ô trống |
| Nullable → bắt buộc | Compatible | reader cũ chỉ gặp ít trường hợp hơn |
| **Thêm** giá trị enum | Breaking | `match` đầy đủ trên tập cũ gặp giá trị lạ (Avro chịu được chỉ khi reader khai báo default) |
| Bớt giá trị enum | Compatible | reader cũ chỉ gặp ít giá trị hơn |

Theo hướng BACKWARD, nhiều dòng **đảo ngược**: int → long là an toàn (reader mới `long` đọc được dữ liệu `int` cũ — đúng quy tắc promotion writer int → reader
long của Avro), thêm trường bắt buộc lại vỡ (dữ liệu cũ không có nó). Bản đầu của lab ghi "int → long: Compatible" mà không nói hướng; review chéo đưa ra phản ví
dụ `2147483648` và bản ghi có trường mới bị chính validator cũ từ chối. Giờ test `CAM KẾT của compatibility` sinh bản ghi biên (`Int.MaxValue`, `Int.MaxValue + 1`,
`Long.MaxValue`, ô trống, từng giá trị enum) cho 9 cặp thay đổi và kiểm tra: phán quyết Compatible ⇔ reader cũ đọc được mọi bản ghi biên hợp lệ của writer mới.

**Contract kiểm tra KIỂU, không thay quy tắc nghiệp vụ.** `quantity = 0` hợp lệ theo contract (là int) nhưng pipeline loại (phải 1..1000). Ngược lại, output thật
ở mục 9 cho thấy contract v1 báo `status='paid'` là vi phạm (enum phân biệt hoa thường) trong khi pipeline chấp nhận và chuẩn hóa thành `PAID`. Hai lớp này
**khác mục đích**: contract đo producer có giữ lời không; validation của pipeline quyết định dòng nào dùng được. Đếm vi phạm contract theo producer là chỉ số
chất lượng của nguồn, kể cả khi pipeline "cứu" được dòng đó.

**Lỗi thiết kế:** contract chỉ là trang wiki (không máy nào kiểm tra); consumer tự đặt contract mà producer không biết; đổi contract không qua review như đổi API.

### 3.5. Golden dataset và regression test

**Golden dataset** (snapshot test, Chương 10) là output đã được **người** duyệt, lưu trong git; test so output mới với nó. Khác → test đỏ, ghi file `.actual`
cạnh golden để `diff`. Thay đổi **có chủ đích** → chạy với `UPDATE_GOLDEN=1`, rồi review diff golden trong pull request như review code.

**Golden chỉ đáng tin khi:**

1. **Tính tay trước khi chạy.** Nếu sinh golden bằng chính code đang test, golden chỉ chứng minh "code giống code". Ở lab, bảng mục 6 được tính tay rồi mới
   chạy — khớp ngay lần đầu là bằng chứng độc lập.
2. **Nhỏ đủ để người kiểm tra được** (13 dòng input) nhưng **bao phủ mọi ca khó**: trùng tuyệt đối, phiên bản mới thay bản cũ, xung đột cùng `updated_at`,
   dữ liệu đến trễ (O8 ngày 15 đến ở lô 16), khách không có trong dimension, dòng hỏng nhiều lỗi, chuẩn hóa hoa/thường và khoảng trắng.
3. **Output tất định**: sắp xếp theo khóa toàn phần, định dạng số cố định, không chứa thời điểm chạy. Nếu không, golden đỏ ngẫu nhiên và bị tắt.

**Regression test.** Golden bắt lỗi mà unit test bỏ sót. Test `regression` trong lab mô phỏng một "tối ưu" tưởng vô hại: bỏ dedup theo phiên bản, chỉ
`distinctBy(lineKey, payload)`. Unit test của `validate` vẫn xanh, nhưng golden đỏ với diff thật (mục 9): đơn O1 đã hoàn tiền vẫn bị tính, đơn O7 đang xung đột
lại xuất hiện.

**Lỗi thiết kế:** `UPDATE_GOLDEN=1` chạy tự động trong CI (golden luôn xanh, vô dụng); golden hàng nghìn dòng không ai đọc diff; golden chứa ngày giờ chạy.

### 3.6. Property test cho tính chất của pipeline

Golden kiểm tra **một** bộ dữ liệu. Property test kiểm tra một **quy luật** trên hàng trăm bộ sinh ngẫu nhiên (ScalaCheck, mặc định 100 lượt mỗi property).
Pipeline dữ liệu có những quy luật rất rõ — và chính là các yêu cầu "test late data / duplicates / reruns / backfills" của đề bài:

| Tính chất | Phát biểu | Bắt được lỗi gì |
|---|---|---|
| Không phụ thuộc thứ tự | `run(shuffle(xs)) == run(xs)` | sắp xếp theo khóa thiếu trường, `head` của `groupBy` |
| Chạy lại (rerun) | `served(run(xs ++ xs)) == served(run(xs))` | append thay vì overwrite, đếm đôi |
| Trùng ở lô sau | thêm bản gửi lại → output phục vụ không đổi, chỉ đếm trùng tăng | dedup theo `batch` thay vì theo nội dung |
| Đến trễ | thêm bản **cũ hơn** đến muộn → không đổi | "bản đến sau thắng" thay vì "`updated_at` lớn nhất thắng" |
| Backfill/replay | trạng thái tăng dần qua nhiều cách chia lô/thứ tự sinh ngẫu nhiên ≡ tính lại toàn bộ — so cả `latest` lẫn `conflicts` | trạng thái gọn bỏ sót xung đột |
| Bảo toàn | rejected + trùng + thay thế + xung đột + fact = số dòng input | dòng "biến mất" giữa các bước |

**Bí quyết là bộ sinh.** Sinh ngẫu nhiên "tự nhiên" (id từ 1 tới 1 triệu) gần như không bao giờ tạo trùng hay xung đột — property xanh mà không kiểm gì. Bộ sinh
trong lab **cố ý hẹp**: 6 mã đơn, 2 SKU, 3 mốc thời gian, 3 lô, 10% số lượng 0, 11% trạng thái hỏng — để trùng, hòa và trễ xảy ra **thường xuyên**.

**Phân biệt output phục vụ và số đếm.** `served = (facts, daily, conflicts)` — thứ người dùng đọc. Số bản trùng **được phép** đổi khi gửi lại; nếu so cả nó,
property "rerun" sai về mặt đặc tả chứ không phải code sai.

**Property không phải chứng minh.** 100 mẫu ngẫu nhiên mỗi lần chạy là **bằng chứng**, không phải "đúng với mọi input". Hai cách làm nó mạnh hơn: đo **độ phủ**
(`Prop.collect` ghi nhãn "có xung đột / không xung đột" để biết ca khó thật sự xuất hiện), và chuyển mỗi phản ví dụ từng tìm được thành **regression test cố định**.
Reviewer còn duyệt **toàn bộ** chuỗi độ dài 0..4 từ 7 biến thể cùng khóa (20.926 tổ hợp) — với không gian nhỏ, liệt kê hết rẻ hơn và chắc hơn sinh ngẫu nhiên.

**Lỗi thiết kế:** property quá yếu (chỉ "không ném exception"); bộ sinh rộng nên không chạm ca khó; không ghi seed khi đỏ (munit-scalacheck in sẵn
`Failing seed` — dán vào `scalaCheckInitialSeed` để tái hiện, mục 11).

### 3.7. Drift: thay đổi ngoài mong đợi trên dữ liệu thật

**Vấn đề.** Trên dữ liệu thật không có đáp án tính tay. Làm sao biết hôm nay "lạ"? So **hồ sơ** (profile) của output với lần chạy trước: số dòng, tổng doanh thu,
tỷ trọng từng segment, tỷ trọng UNKNOWN, số ngày. Vượt **ngưỡng** → cảnh báo.

Ví dụ thật: mất khách C9 (ví dụ một file nguồn không đến) làm doanh thu đổi 2,05% — vượt ngưỡng 1%. Không check từng dòng nào bắt được, vì mọi dòng còn lại đều đúng.

Dùng drift cho cả **hai** loại thay đổi:

- **Dữ liệu đổi, code giữ nguyên** (hôm qua vs hôm nay) → nguồn có vấn đề.
- **Code đổi, dữ liệu giữ nguyên** (chạy bản cũ và bản mới của job trên cùng snapshot production, so profile) → thay đổi code có tác động lớn hơn dự kiến.
  Đây là cách kiểm thử regression khi golden mẫu quá nhỏ để đại diện.

`Profile` ghi cả số dòng nhưng `Drift.compare` lab chỉ so doanh thu, tỷ trọng segment, tỷ trọng UNKNOWN và số ngày — số dòng được theo dõi để đọc, chưa đặt
ngưỡng. Baseline doanh thu bằng 0 là **quy ước chính sách**: 0 → 0 không cảnh báo, 0 → dương coi là đổi 100%.

**Ngưỡng là quyết định nghiệp vụ**: quá chặt → cảnh báo giả mỗi ngày (cuối tuần, ngày lễ doanh thu đổi 30% là bình thường); quá lỏng → vô dụng. Bắt đầu
bằng ngưỡng rộng, ghi lại lịch sử profile, siết dần theo dữ liệu thật; so với **cùng ngày tuần trước** khi có tính mùa vụ.

### 3.8. Integration và end-to-end test

**Integration test** kiểm tra một ranh giới thật: đọc file, parse CSV, ghi PostgreSQL, Kafka (Chương 15, 28 — tách riêng, `assume` khi thiếu môi trường).
**End-to-end** chạy cả pipeline từ input tới output **qua đúng đường code production** (ở lab: chép input ra thư mục tạm, đọc bằng `DataFiles`, chạy, so golden).
E2E bắt lỗi mà unit test không thấy: đường dẫn, encoding, header, thứ tự cột.

Áp dụng cho Spark: cùng sáu tầng, khác công cụ — unit test hàm `DataFrame => DataFrame` trên `local[2]` với vài dòng (Chương 22–24), check bằng truy vấn tập
(`groupBy(key).count > 1`), golden ghi `orderBy` toàn phần ra CSV một file, e2e qua `spark-submit` (Project 3). Lab này giữ Scala thuần để mỗi test chạy trong
mili giây; khi một tính chất đã được chứng minh ở đây, Spark chỉ cần test rằng nó **giữ nguyên** tính chất (cùng golden).

**Kim tự tháp:** nhiều unit + property (rẻ, nhanh), vừa đủ check, ít e2e (chậm, khó debug). E2E đỏ thì phải có test nhỏ hơn chỉ ra chỗ hỏng.

## 4. Thuật ngữ tiếng Anh

| Thuật ngữ | Nghĩa trong bài |
|---|---|
| Data quality check | kiểm tra trên dữ liệu (thường là trên tập) mỗi lần pipeline chạy |
| Uniqueness / not null / accepted values | khóa không trùng / trường không rỗng / giá trị thuộc tập cho phép |
| Referential integrity | mọi khóa ngoại tồn tại trong bảng được tham chiếu |
| Freshness | dữ liệu mới nhất không quá cũ so với thời điểm kiểm tra |
| Data contract | thỏa thuận producer–consumer về schema + ý nghĩa, máy kiểm tra được |
| Breaking / compatible change | thay đổi làm vỡ / không làm vỡ consumer |
| Golden dataset / snapshot test | output đã được duyệt, lưu lại để so |
| Regression test | test bắt một hành vi đã đúng nay sai đi |
| Property-based test | test một quy luật trên nhiều input sinh ngẫu nhiên |
| Shrinking | ScalaCheck thu nhỏ phản ví dụ về dạng tối giản |
| Drift / profile | thay đổi phân phối của output / hồ sơ tóm tắt output |
| Reconciliation | đối soát số dòng/tổng giữa input và các ngăn output |
| Backfill / replay / rerun | tính lại dữ liệu quá khứ / đưa lại event cũ / chạy lại cùng input |
| Write-audit-publish | ghi vào chỗ tạm → check → mới publish (Chương 24) |

## 5. Ví dụ đơn giản

Một check uniqueness trên tập, viết bằng chính kiểu của lab:

```scala
import vn.scalaeng.quality.*

val ids = Seq("O1", "O2", "O1")
val r = Check.unique[String, String]("order_id duy nhất", Severity.Error)(identity).run(ids)
// r.failed == 2 (hai dòng thuộc khóa trùng), r.samples == List("O1"), r.passed == false
```

Quy tắc này **không viết được** dưới dạng `String => Boolean`: dòng `"O1"` tự nó không sai — chỉ sai khi có một `"O1"` khác. Đó là lý do check nhận cả tập.

## 6. Ví dụ thực tế — tính tay hai lô đơn hàng

Input ([`data/input/`](quality-lab/data/input/)): 13 dòng trong hai lô `ingest_date=2026-10-15` (7 dòng) và `2026-10-16` (6 dòng); dimension 3 khách C1–C3.

| # | Lô | Dòng | Số phận | Lý do |
|---|---|---|---|---|
| 1 | 15 | O1 `c1` PAID ` kb-01 ` 2×450000 @01:00 | **thay thế** | bản 16/10 09:00 mới hơn (#8) |
| 2 | 15 | O1 C1 PAID MS-01 1×250000 | fact | |
| 3 | 15 | O2 C2 `paid` KB-01 50×400000 | fact | chuẩn hóa `paid` → PAID |
| 4 | 15 | O2 giống hệt #3 | **trùng** | trùng tuyệt đối |
| 5 | 15 | O3 C3 HP-01 **0**×990000 | **rejected** | quantity 1..1000 |
| 6 | 15 | O4 **C9** KB-01 1×450000 | fact (UNKNOWN) | C9 không có trong dimension |
| 7 | 15 | O5: thiếu khách, `2026-13-01`, SHIPPED, thiếu sku, giá −5, `not-a-time` | **rejected** | 6 lỗi |
| 8 | 16 | O1 C1 **REFUNDED** KB-01 @16/10 09:00 | fact | hoàn tiền đến muộn sửa ngày 15 |
| 9 | 16 | O6 C2 MS-01 3×250000 | fact | |
| 10–11 | 16 | O7 HP-01 PAID và CANCELLED **cùng** `updated_at` | **xung đột** (2) | không biết bản nào đúng |
| 12 | 16 | O8 C1 KB-01 1×450000, **day = 15/10** | fact | dữ liệu trễ: ngày 15 đến ở lô 16 |
| 13 | 16 | O9 quantity `abc` | **rejected** | |

Đối soát: 3 rejected + 1 trùng + 1 thay thế + 2 xung đột + 6 fact = **13** ✔.

Doanh thu ngày (chỉ PAID, theo `day` và segment):

| day | segment | Tính | revenue | dòng | đơn |
|---|---|---|---|---|---|
| 15/10 | Retail | O1/MS-01 250000 + O8 450000 (O1/KB-01 đã REFUNDED) | 700000 | 2 | 2 |
| 15/10 | UNKNOWN | O4 450000 | 450000 | 1 | 1 |
| 15/10 | Wholesale | O2 50×400000 | 20000000 | 1 | 1 |
| 16/10 | Wholesale | O6 3×250000 | 750000 | 1 | 1 |

Tổng 21.900.000. Bảng này được ghi vào [`golden/daily_revenue.csv`](quality-lab/golden/daily_revenue.csv) **trước** khi chạy test lần đầu; test xanh ngay.

## 7. Code đầy đủ

```
quality-lab/
├── project.scala, build.sbt, project/build.properties
├── contracts/orders.v1.json, orders.v2.json      # data contract hai phiên bản
├── data/customers.csv, data/input/ingest_date=*/orders.csv
├── golden/daily_revenue.csv, rejected.csv, conflicts.csv   # tính tay
└── src/
    ├── main/scala/vn/scalaeng/quality/
    │   ├── Pipeline.scala     # validate, dedup hai tầng, facts, daily, Incremental (trạng thái gọn)
    │   ├── Checks.scala       # Check, CheckResult, Severity, OutputChecks
    │   ├── Contract.scala     # parse contract (circe), validate (producer) / read (reader), compatibility FORWARD
    │   ├── Drift.scala        # Profile, Tolerance, Drift.compare
    │   ├── Files.scala        # đọc lô theo thư mục (bỏ BOM, từ chối dòng thừa cột), ghi CSV ổn định
    │   └── QualityDemo.scala  # chạy toàn bộ, in báo cáo
    └── test/scala/vn/scalaeng/quality/
        ├── QualitySuite.scala   # 12 test: unit, check, contract ×3, golden, regression, drift, e2e, DataFiles, late data
        ├── PropertySuite.scala  # 6 property: thứ tự, rerun, trùng, trễ, backfill, bảo toàn
        └── Golden.scala         # so golden, ghi .actual, UPDATE_GOLDEN=1
```

Chạy:

```
cd 11-data-platform/quality-lab
scala-cli test .                                             # hoặc: sbt test
scala-cli run . --main-class vn.scalaeng.quality.QualityDemo
```

Trên Windows, nếu chữ tiếng Việt hiện thành `?`, thêm `--java-opt -Dstdout.encoding=UTF-8`.

## 8. Giải thích code

- **`Pipeline.validate`** gom lỗi bằng `List[Option[String]].flatten` (đủ cho 9 trường; Chương 20 dùng `ValidatedNec` khi có quy tắc phụ thuộc nhau).
  Chuẩn hóa `customer_id`, `sku` bằng `toUpperCase(Locale.ROOT)` — không phụ thuộc locale máy (Chương 19).
- **`Order.payload`** tách nội dung nghiệp vụ khỏi `batch`: cùng bản ghi đến ở hai lô vẫn là **một** — nền tảng cho "trùng ở lô sau không đổi output".
- **`Order.sortKey`** gồm **mọi** trường. Comment giải thích vì sao: sắp xếp theo khóa thiếu trường thì các bản "hòa" giữ thứ tự của `groupBy` (bảng băm) — output
  phụ thuộc thứ tự input. Mục 10 cho thấy property test bắt lỗi này thật.
- **`Pipeline.dedup`**: tầng 1 gộp trùng tuyệt đối (giữ lô **sớm nhất** — tất định), tầng 2 giữ `updated_at` lớn nhất mỗi dòng hàng; nhiều bản mới nhất khác
  nội dung → **xung đột**, không chọn bừa.
- **`Incremental`**: trạng thái gọn (mỗi dòng hàng nhớ `updated_at` lớn nhất + các nội dung ở thời điểm đó) để xử lý từng lô mà không đọc lại lịch sử.
  Nhánh `o.updatedAt == t` là chỗ dễ sai nhất — mục 10.
- **`Check`** là case class chứa hàm: danh sách check là **dữ liệu**, in báo cáo, lọc theo mức, thêm/bớt mà không đổi pipeline.
- **`Contract.compatibility`** so hai contract theo từng trường (hướng FORWARD); trả về danh sách thay đổi có `Impact` — CI của producer chặn merge nếu có
  `Breaking` mà không tăng phiên bản chính. `validate` và `read` dùng chung `fieldErrors`; khác nhau đúng một chỗ: trường lạ.
- **`DataFiles.records`** bỏ BOM `U+FEFF` ở header (`trim` không bỏ nó), điền rỗng cột thiếu, và đặt giá trị **thừa** vào khóa `Contract.ExtraColumns` để cả contract
  lẫn pipeline từ chối dòng đó.
- **`Golden.check`** chuẩn hóa `\r\n` → `\n` (git trên Windows có thể đổi xuống dòng khi checkout) rồi so; khác thì ghi `.actual` và trả diff dòng.
- **`Profile` / `Drift`** dùng `BigDecimal` scale 4, `HALF_EVEN` — tỷ lệ in ổn định, so ngưỡng không bị sai số nhị phân.
- **`build.sbt`**: `Test / fork := true` + `Test / baseDirectory` để test đọc `data/`, `golden/` theo đường dẫn tương đối dù chạy từ đâu.

## 9. Output mong đợi (output thật)

`scala-cli test .` (và `sbt test`):

```
Test run vn.scalaeng.quality.PropertySuite finished: 0 failed, 0 ignored, 6 total
Test run vn.scalaeng.quality.QualitySuite finished: 0 failed, 0 ignored, 12 total
[info] Passed: Total 18, Failed 0, Errors 0, Passed 18
```

`scala-cli run . --main-class vn.scalaeng.quality.QualityDemo`:

```
== Contract orders v1 (chủ sở hữu team-checkout) trên 13 bản ghi input
   lô 2026-10-15 O2: status='paid': không phải enum {PAID,REFUNDED,CANCELLED}
   lô 2026-10-15 O2: status='paid': không phải enum {PAID,REFUNDED,CANCELLED}
   lô 2026-10-15 O5: customer_id: bắt buộc; day='2026-13-01': không phải date; status='SHIPPED': không phải enum {PAID,REFUNDED,CANCELLED}; sku: bắt buộc; updated_at='not-a-time': không phải timestamp
   lô 2026-10-16 O9: quantity='abc': không phải int
== Tương thích v1 → v2 (góc nhìn người tiêu thụ)
   Breaking   quantity: int → long
   Breaking   sku: bắt buộc → nullable
   Breaking   status: thêm giá trị PARTIALLY_REFUNDED
   Compatible channel: được thêm (nullable) — reader cũ bỏ qua
== Pipeline: fact=6 rejected=3 conflicts=2 trùng=1 thay-thế=1
== Checks trên output
   PASS Error fact: (order_id, sku) duy nhất             0/6 
   FAIL Warn  fact: customer_id tồn tại                  1/6 C9
   PASS Error fact: doanh thu dòng 1..10 tỷ              0/6 
   PASS Error fact: có dữ liệu trong 2 ngày gần nhất     0/6 
   PASS Error daily: (day, segment) duy nhất             0/4 
   PASS Error daily: 1..1000 dòng                        0/4 
   PASS Error daily: orders <= order_lines               0/4 
day,segment,revenue_vnd,order_lines,orders
2026-10-15,Retail,700000,2,2
2026-10-15,UNKNOWN,450000,1,1
2026-10-15,Wholesale,20000000,1,1
2026-10-16,Wholesale,750000,1,1
== Drift: profile hiện tại Profile(4,21900000,0.0205,Map(Wholesale -> 0.9475, Retail -> 0.0320, UNKNOWN -> 0.0205),2)
   giả sử lần chạy sau MẤT khách C9 → List(doanh thu đổi 2.0500% (21900000 → 21450000) > 1.00%)
```

Đọc kỹ:

- Contract bắt **4** bản ghi, pipeline loại **3** — tập khác nhau: O2 (`paid`) vi phạm contract nhưng pipeline cứu được; O3 (`quantity = 0`) đúng kiểu nên qua contract
  nhưng pipeline loại. Mục 3.4.
- Check `customer_id tồn tại` FAIL nhưng là **Warn** — pipeline vẫn publish.
- Golden diff thật khi "tối ưu" bỏ dedup phiên bản (test `regression`):

```
daily_revenue.csv khác golden (đã ghi golden\daily_revenue.csv.actual):
- 2026-10-15,Retail,700000,2,2
+ 2026-10-15,Retail,1600000,3,2
+ 2026-10-16,Retail,990000,1,1
```

  Dòng thứ hai: O1/KB-01 2×450000 đã hoàn tiền vẫn bị cộng (700000 + 900000). Dòng thứ ba: O7 đang xung đột (PAID/CANCELLED) bị coi như PAID.

## 10. Lỗi thường gặp (thật)

**Lỗi 1 — Sắp xếp output theo khóa thiếu trường → output phụ thuộc thứ tự input.** Đổi `sortKey` thành `(orderId, sku)` (trông hợp lý: đó là khóa dòng hàng).
Unit test, golden, e2e vẫn xanh. Property test đỏ:

```
==> X vn.scalaeng.quality.PropertySuite.đến trễ: thêm bản CŨ HƠN mọi bản hiện có (updated_at 14/10, đến ở lô 18) của các dòng hàng hợp lệ → output phục vụ không đổi
Failing seed: Xw3uLdjJ9MfCygtOP1xNk90s7fDtO63TN2E-xR52CwF=
Falsified after 20 passed tests.
> ARG_0: Vector(RawOrder(2026-10-17,O4,C9,2026-10-15,PAID,MS-01,5,250,2026-10-15T01:00:00Z), RawOrder(2026-10-17,O5,C9,2026-10-15,REFUNDED,KB-01,1,250,2026-10-15T05:00:00Z), RawOrder(2026-10-16,O4,C9,2026-10-16,PAID,MS-01,1,100,2026-10-15T01:00:00Z))
```

Phản ví dụ đã được **shrink** còn 3 dòng: hai bản O4/MS-01 cùng `updated_at` nhưng khác nội dung → xung đột; danh sách `conflicts` sắp theo `(O4, MS-01)` thì
hai bản **hòa**, thứ tự giữa chúng do `groupBy` quyết định — thêm vài dòng không liên quan là bảng băm đổi thứ tự. Điều đáng học: lỗi bị property "đến trễ"
bắt, **không phải** property "thứ tự" — mỗi lần chạy ScalaCheck sinh dữ liệu khác, nên có nhiều property chạm cùng vùng là một lớp bảo vệ. Sửa: khóa sắp xếp
**toàn phần** (mọi trường).

**Lỗi 2 — Trạng thái tăng dần bỏ qua bản cùng `updated_at`.** Trong `Incremental.step`, coi bản có `updated_at` bằng bản đang giữ là "không mới hơn → bỏ qua"
(điều kiện `isAfter` quen thuộc). Kết quả thật:

```
==> X vn.scalaeng.quality.PropertySuite.backfill / replay: trạng thái tăng dần (gọn) qua MỌI cách chia lô, theo MỌI thứ tự lô ≡ tính lại toàn bộ
Falsified after 3 passed tests.
> ARG_0: Vector(RawOrder(2026-10-17,O3,C9,2026-10-15,PAID,KB-01,3,250,2026-10-15T05:00:00Z), RawOrder(2026-10-15,O3,C1,2026-10-16,PAID,KB-01,3,100,2026-10-15T05:00:00Z))
> ARG_1: 1
> ARG_2: 0
```

Hai bản O3/KB-01 cùng `updated_at`, khác nội dung: tính lại toàn bộ thấy **xung đột** (không publish); xử lý tăng dần giữ bản **đến trước** và publish nó —
kết quả phụ thuộc lô nào được xử lý trước. Đây đúng là lỗi chỉ lộ ra khi **backfill** theo thứ tự khác lúc chạy hằng ngày.

**Lỗi 3 — Chưa có golden, hoặc sinh golden bằng chính code.** Lần chạy đầu của lab (trước khi tính tay):

```
==> X vn.scalaeng.quality.QualitySuite.golden: daily_revenue, rejected, conflicts của dữ liệu mẫu tính tay
munit.FailException: src\test\scala\vn\scalaeng\quality\QualitySuite.scala:73 chưa có golden golden\daily_revenue.csv — chạy với UPDATE_GOLDEN=1 sau khi tự kiểm tra output
```

Cám dỗ là chạy ngay `UPDATE_GOLDEN=1`. Đừng: golden sinh ra lúc đó chỉ ghi lại hành vi hiện tại, kể cả lỗi. Tính tay (mục 6), ghi file, **rồi** chạy.

**Lỗi 4 — Kỳ vọng sai trong chính test vì hiểu sai đặc tả.** Khi viết lời giải bài 2, tác giả kỳ vọng 2 fact Retail (nhìn bảng daily) và nhận:

```
values are not the same
=> Obtained
List(
  "retail",
  "retail",
  "retail"
)
```

Fact giữ **mọi** trạng thái (O1/KB-01 REFUNDED là fact Retail thứ ba); chỉ bảng daily lọc PAID. Khi test đỏ, câu hỏi đầu tiên là "đặc tả nói gì?" chứ không phải
"sửa code cho xanh". Ghi rõ đặc tả từng bảng (fact gồm gì, daily gồm gì) ngay cạnh golden.

**Lỗi 5 — Contract và pipeline lệch nhau mà không ai biết.** Output mục 9: contract v1 nói enum `PAID` (hoa), pipeline nhận `paid`. Nếu chỉ chạy pipeline, vi phạm
của producer bị che; nếu chặn theo contract, đơn O2 (20 triệu) mất khỏi báo cáo. Quyết định có chủ đích: **đếm** vi phạm contract theo producer (chỉ số nguồn),
**xử lý** theo validation của pipeline — và ghi quyết định đó vào contract (`"case_insensitive": true` hoặc sửa producer).

**Lỗi 6 — Parser CSV "tự viết" xóa mất dữ liệu sai** (review chéo tìm ra). Bản đầu của `DataFiles.records` ghép giá trị với header bằng `zipAll` rồi lọc tên
cột rỗng: một dòng `…,2026-10-15T01:00:00Z,UNDECLARED` (thừa một cột) trở thành bản ghi **hợp lệ**, giá trị thừa biến mất trước khi contract hay validation kịp
thấy. Không có exception nào — chỉ test tấn công mới lộ. Giờ dòng đó bị từ chối với lý do thật: `dòng có giá trị thừa ngoài header: 'UNDECLARED'`. Vòng 2 của review tìm thêm biến thể: dòng kết thúc bằng
`,` (cột thừa **rỗng**) vẫn lọt, vì pipeline kiểm **nội dung** cột thừa sau `trim` thay vì kiểm **sự hiện diện**. Giờ `RawOrder.extra` là `Option[String]`:
`Some("")` (dấu phẩy cuối), `Some("   ")` hay `Some(",")` đều bị từ chối. Tương tự, BOM
UTF-8 ở đầu file (Excel hay ghi) làm header thành `\uFEFForder_id` và **mọi** dòng báo `order_id: bắt buộc`.

**Lỗi 7 — Khóa sắp xếp quá 9 trường không biên dịch.** Thêm trường thứ 10 (`extra`) vào `Rejected.sortKey`:

```
-- [E172] Type Error: ...Pipeline.scala:103:59
103 |    Output(f, daily(f), rejected.toVector.sortBy(_.sortKey), d.conflicts, d.exactDuplicates, d.superseded)
    |                                                           ^
    |No given instance of type Ordering[
    |  (String, String, String, String, String, String, String, String, String,
    |    String)
    |] was found for parameter ord of method sortBy in trait SeqOps.
```

Thư viện chuẩn chỉ định nghĩa `Ordering` cho tuple **tới 9 phần tử**. Lồng tuple: `((9 trường đầu), trường thứ 10)` — `Ordering` của tuple lồng tự suy ra.

**Lỗi 8 — Golden đỏ trên Windows dù output đúng** (*phòng ngừa, chưa tái hiện trong lab*). Git có thể checkout file với `\r\n` (tùy `core.autocrlf`); so chuỗi
thẳng sẽ khác ở mọi dòng. `Golden.check` chuẩn hóa xuống dòng ở **cả hai** phía (golden và output) trước khi so. Lỗi cùng họ đã gặp thật ở Project 1: đường dẫn golden sai khi sbt fork JVM test —
`Test / baseDirectory` trong `build.sbt` xử lý.

## 11. Cách debug

1. **Property đỏ:** đọc `ARG_0` (đã shrink) trước `ARG_0_ORIGINAL`. Chép `Failing seed` vào `override def scalaCheckInitialSeed = "..."` để chạy lại đúng phản ví dụ,
   sửa, rồi **xóa** override (để property tiếp tục tìm ca mới). Đưa phản ví dụ thành một unit test cố định (regression).
2. **Golden đỏ:** `diff golden/daily_revenue.csv golden/daily_revenue.csv.actual`. Với mỗi dòng khác, tính tay lại từ bảng mục 6 — đừng mặc định golden đúng hay
   code đúng.
3. **Check đỏ trong production:** đọc `samples`, truy ngược về lô (`batch`) và file nguồn. Check không có mẫu là check khó debug — thêm mẫu.
4. **Drift:** so profile theo từng chiều (ngày, segment) để khoanh vùng; kiểm tra số dòng input từng lô trước (mất file là nguyên nhân phổ biến nhất).
5. **E2E đỏ, unit xanh:** nghi đường dẫn, encoding, header, xuống dòng — chạy demo với `-Dstdout.encoding=UTF-8` và in `DataFiles.batches(...).size`.

## 12. Bài tập

1. **(Cơ bản)** Viết check theo **tỷ lệ** `maxShare`: vi phạm khi tỷ lệ dòng thỏa điều kiện vượt ngưỡng. Áp cho tỷ lệ fact `UNKNOWN` với ngưỡng 20% và 10%.
2. **(Cơ bản)** Viết check **accepted values** cho `segment ∈ {Retail, Wholesale, UNKNOWN}`; cho thấy nó bắt được lỗi `retail` viết thường ở dimension.
3. **(Trung bình)** Sinh bản ghi **hợp lệ từ contract** (`sampleRecord(contract, i)`) và chứng minh mọi bản sinh ra qua `Contract.validate` của chính contract đó,
   cho cả v1 và v2; bản ghi của v2 vi phạm v1 ở đâu?
4. **(Trung bình)** Đưa **đối soát** "mọi dòng vào đúng một ngăn" thành một `Check[Output]` chạy trong production (không chỉ trong property test).
5. **(Trung bình)** Drift trên **tỷ lệ bị loại**: tính tỷ lệ rejected/input, cảnh báo khi tăng quá ngưỡng. Mô phỏng nguồn đổi định dạng ngày cho 10 dòng.
6. **(Nâng cao)** Property: `Incremental.step` **idempotent theo lô** — áp lại cùng lô (replay) không đổi trạng thái.
7. **(Nâng cao)** **Backfill theo ngày**: bảng daily đang phục vụ được tính trước khi lô 16/10 đến; viết `backfill(serving, recomputed, days)` thay đúng các ngày cần
   tính lại, và test rằng ngày 16/10 không bị động tới.

## 13. Đáp án và lời giải

[`solutions/ch33-solutions.md`](../solutions/ch33-solutions.md), code [`solutions/ch33-code/`](../solutions/ch33-code/) — 7/7 test pass.

## 14. Câu hỏi kiểm tra

1. Vì sao pipeline có 100% test xanh vẫn ghi ra số sai? Kể ba nguyên nhân.
2. Vì sao uniqueness không viết được dưới dạng hàm trên một dòng?
3. Khi nào một check nên là Warn thay vì Error? Cho ví dụ từ lab.
4. Contract kiểm tra `quantity` là int. Vì sao `quantity = 0` vẫn qua contract, và lớp nào phải bắt nó?
5. Thêm một giá trị enum là breaking hay compatible? Trả lời theo hai hướng reader/writer.
6. Ba điều kiện để golden dataset đáng tin là gì? Vì sao không được sinh golden bằng chính code đang test?
7. Vì sao bộ sinh trong `PropertySuite` cố ý dùng ít khóa và ít mốc thời gian?
8. `served(o)` loại số bản trùng khỏi phép so. Nếu không loại, property "rerun" sẽ ra sao?
9. Drift bắt được gì mà golden không bắt được, và ngược lại?
10. Property "trạng thái tăng dần ≡ tính lại toàn bộ" liên quan gì tới backfill?

<details><summary>Gợi ý đáp án</summary>

1. Nguồn đổi định dạng; dimension trùng khóa nhân doanh thu; mất file một lô — test chạy trên dữ liệu mẫu cố định, không trên dữ liệu hôm nay.
2. Một dòng tự nó không vi phạm; vi phạm là quan hệ giữa các dòng.
3. Khi chặn publish gây hại hơn số liệu thiếu sót có đánh dấu: khách C9 chưa đồng bộ → gán UNKNOWN, Warn, theo dõi tỷ lệ.
4. Contract kiểm tra kiểu; quy tắc nghiệp vụ (1..1000) thuộc validation của pipeline.
5. Consumer viết code theo bản cũ đọc dữ liệu mới → có thể vỡ (giá trị lạ); dữ liệu cũ đọc bằng code mới → không vỡ. Phải nói rõ hướng.
6. Tính tay trước khi chạy; nhỏ nhưng phủ ca khó; output tất định. Sinh bằng code thì golden chứa luôn lỗi của code.
7. Để trùng, hòa, đến trễ xảy ra thường xuyên — bộ sinh rộng không chạm ca khó.
8. Đỏ, vì số bản trùng tăng gấp đôi — đó là sai đặc tả của test, không phải lỗi code.
9. Drift: thay đổi trên dữ liệu thật không có đáp án. Golden: sai lệch chính xác từng dòng trên mẫu có đáp án.
10. Backfill xử lý lại các lô theo thứ tự/cách chia khác lúc chạy hằng ngày; property chứng minh kết quả không phụ thuộc điều đó.
</details>

## 15. Mini project

Thêm **cổng chất lượng** (quality gate) cho lab theo kiểu write-audit-publish (Chương 24): chạy pipeline, chạy mọi check Error + đối soát + drift so với
profile lần trước (lưu JSON), chỉ ghi `output/daily_revenue.csv` khi không có lỗi Error; ghi `output/quality_report.json` (mọi `CheckResult`, profile, drift)
trong **mọi** trường hợp. Test: (a) dữ liệu mẫu → publish; (b) thêm một dòng trùng khóa ở dimension → không publish, report nêu khóa; (c) xóa lô 16/10 →
drift cảnh báo, và freshness đỏ **khi** đặt `maxLagDays = 1` (với `asOf` 17/10 và ngưỡng 2 ngày như demo, ngày mới nhất 15/10 vẫn nằm đúng biên — reviewer chạy
thật: `0` vi phạm). Phân biệt **freshness** (dữ liệu mới nhất có đủ mới?) với **completeness** (mọi lô kỳ vọng đã đến?) — thêm một check thứ hai cho cái sau. Viết README ngắn: ai nhận cảnh báo nào, ngưỡng được chọn vì sao.

## 16. Nguồn tài liệu chính thức

- munit + ScalaCheck: https://scalameta.org/munit/docs/integrations/scalacheck.html
- ScalaCheck User Guide (generator, shrinking, seed): https://github.com/typelevel/scalacheck/blob/main/doc/UserGuide.md
- circe: https://circe.github.io/circe/ · circe-parser 0.14.17 trên Maven Central: https://repo1.maven.org/maven2/io/circe/circe-parser_3/0.14.17/
- dbt data tests (unique, not_null, accepted_values, relationships): https://docs.getdbt.com/docs/build/data-tests
- Deequ (data quality trên Spark): https://github.com/awslabs/deequ
- Great Expectations: https://docs.greatexpectations.io/docs/
- Open Data Contract Standard: https://bitol-io.github.io/open-data-contract-standard/latest/
- Avro schema resolution: https://avro.apache.org/docs/1.12.0/specification/
- Confluent Schema Registry — compatibility types: https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html

(Các link truy cập được ngày 17/10/2026. Deequ, Great Expectations, Soda, dbt chỉ được nhắc ở mức khái niệm — chương này **không** dùng và không kiểm chứng
phiên bản của chúng.)

## 17. Checklist tự đánh giá

- [ ] Tôi phân biệt được test code và check dữ liệu, và biết mỗi loại chạy ở đâu.
- [ ] Tôi viết được check uniqueness, referential integrity, row count, freshness và chọn Error/Warn có lý do.
- [ ] Tôi viết được data contract và phân loại một thay đổi schema là breaking hay compatible, nói rõ theo hướng nào.
- [ ] Golden dataset của tôi được tính tay trước, nhỏ, phủ ca khó, output tất định.
- [ ] Tôi viết được property test cho thứ tự đến, rerun, trùng, đến trễ, backfill, bảo toàn — với bộ sinh đủ hẹp.
- [ ] Khi property đỏ, tôi dùng phản ví dụ đã shrink và seed để tái hiện, rồi giữ nó thành regression test.
- [ ] Tôi phát hiện được drift trên dữ liệu thật và giải thích được ngưỡng.
- [ ] Tôi có ít nhất một end-to-end test đi qua đúng đường đọc/ghi file của production.
