# Sơ đồ Prerequisite giữa các chương

Sơ đồ dưới đây dùng Mermaid (GitHub, GitLab, IntelliJ, VS Code đều hiển thị được).
Mũi tên `A --> B` nghĩa là **phải học A trước B**.

```mermaid
flowchart TD
  subgraph G1["Giai đoạn 1 — Scala 3 Core"]
    C1[Ch1 Introduction] --> C2[Ch2 Environment]
    C2 --> C3[Ch3 Variables/Types/Expressions]
    C3 --> C4[Ch4 Functions & Methods]
    C4 --> C5[Ch5 OOP]
    C4 --> C6[Ch6 Collections]
    C5 --> C6
    C6 --> C7[Ch7 Functional Programming]
    C5 --> C7
    C7 --> C8A[Ch8A Generics/Variance/Opaque/Extension]
    C8A --> C8B[Ch8B Given/Using/Type class]
  end

  subgraph G2["Giai đoạn 2 — Professional"]
    C9[Ch9 Project Structure]
    C10[Ch10 Testing]
    C11[Ch11 JVM Fundamentals]
  end
  C7 --> C9
  C9 --> C10
  C8A --> C10
  C9 --> C11

  P1{{P1 Scala Data Processor}}
  C10 --> P1

  subgraph BE["Giai đoạn 3 — Backend"]
    C12[Ch12 Backend Fundamentals]
    C16A[Ch16A Concurrency/Future]
    C16B[Ch16B Cats Effect]
    C13[Ch13 Chọn framework]
    C14[Ch14 REST APIs]
    C15[Ch15 Database]
    C17[Ch17 Security & Production]
    C18[Ch18 Architecture]
  end
  C10 --> C12
  C11 --> C16A
  C12 --> C16A
  C16A --> C16B
  C8B --> C16B
  C16B --> C13
  C13 --> C14
  C14 --> C15
  C15 --> C17
  C17 --> C18
  P2{{P2 Backend Service}}
  C18 --> P2

  subgraph DE["Giai đoạn 4 — Data Foundation"]
    C19[Ch19 Data Processing]
    C20[Ch20 Functional Transformation]
    C21[Ch21 Formats & Serialization]
  end
  C10 --> C19
  C19 --> C20
  C20 --> C21

  subgraph SP["Giai đoạn 5 — Spark (Scala 2.13)"]
    C22[Ch22 Spark Fundamentals]
    C23[Ch23 Spark SQL/DataFrame]
    C24[Ch24 Batch Pipeline]
    C25[Ch25 Spark Performance]
    C26[Ch26 Spark Production]
  end
  C21 --> C22
  C11 --> C22
  C22 --> C23 --> C24 --> C25 --> C26
  P3{{P3 Batch Pipeline}}
  C26 --> P3

  subgraph ST["Giai đoạn 6 — Streaming"]
    C27[Ch27 Streaming Fundamentals]
    C28[Ch28 Kafka với Scala]
    C29[Ch29 Streaming Pipeline]
    C30[Ch30 Reliability]
  end
  C21 --> C27
  C16B --> C28
  C27 --> C28 --> C29 --> C30
  C23 --> C29
  P4{{P4 Kafka Event Processing}}
  C30 --> P4

  subgraph PL["Giai đoạn 7 — Platform"]
    C31[Ch31 Pipeline Architecture]
    C32[Ch32 Distributed Processing]
    C33[Ch33 Data Quality]
    C34[Ch34 Performance & Cost]
  end
  C26 --> C31
  C30 --> C31
  C25 --> C32
  C31 --> C33
  C32 --> C34
  P5{{P5 Data Platform}}
  C33 --> P5
  C34 --> P5
  P2 --> P5
  P4 --> P5

  C8C[Ch8C Match types/Inline/Macros — CS]
  C16B -.tùy chọn.-> C8C
```

## Bảng prerequisite dạng văn bản

| Chương | Cần học trước | Lý do |
|---|---|---|
| Ch6 Collections | Ch4, Ch5 | `map/filter` nhận function (Ch4); phần tử thường là case class (Ch5) |
| Ch7 FP | Ch5, Ch6 | ADT dựng từ sealed trait/enum (Ch5); Option/Either dùng API giống collection (Ch6) |
| Ch8B Given/Using | Ch8A | Type class cần generics và variance |
| Ch10 Testing | Ch8A, Ch9 | Test generic code; test cần cấu trúc project sbt |
| Ch16B Cats Effect | Ch8B, Ch16A | Cats dùng type class rất nhiều; cần hiểu Future để thấy vì sao cần IO |
| Ch13 Framework | Ch16B | So sánh framework chủ yếu là so sánh mô hình effect/concurrency |
| Ch22 Spark | Ch11, Ch21 | Spark là ứng dụng JVM phân tán; serialization & schema là cốt lõi |
| Ch28 Kafka | Ch16B, Ch27 | Consumer là chương trình chạy lâu, xử lý concurrency và resource |
| Ch29 Streaming Pipeline | Ch23 | So sánh với Spark Structured Streaming |

## Các lộ trình rút gọn

- **Chỉ Backend:** Ch1–11 → Ch12 → Ch16A → Ch16B → Ch13–15 → Ch17–18 → P2.
  Nên thêm Ch27–28 (event-driven backend).
- **Chỉ Data Engineering:** Ch1–11 (Ch8B mức NB) → Ch19–21 → Ch22–26 → Ch27–30 → Ch31–34 → P1, P3, P4.
  Nên thêm Ch16A (concurrency nền tảng) và Ch15 phần SQL.
- **Full (BE + DE):** theo thứ tự trong [05-learning-order-and-schedule.md](05-learning-order-and-schedule.md).
