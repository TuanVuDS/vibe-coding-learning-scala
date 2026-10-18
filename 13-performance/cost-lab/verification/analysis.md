# Bảng tính lại từ raw T20

Sinh bằng `python 13-performance/cost-lab/analyze.py`; giá compute **giả định** 0,12 đơn vị tiền/node-hour, 1 node. Cost là mô hình capacity, chưa có startup/idle/sink. CPU/wall lấy tổng CPU chia tổng wall qua 21 lượt; các case rất ngắn chịu độ phân giải CPU thô và thread nền JVM. Discarded-result bỏ kết quả nên rate cơ học không phải capacity nghiệp vụ; không định giá case đó.

## serialization

Bối cảnh snapshot: i7-9750H, 12 CPU logic, RAM 34.188.873.728 byte, Windows 11/Adoptium 21.0.12.1, G1, heap 512 MiB. Máy dùng chung; không có nhật ký job nền để xác nhận tác vụ chạy đồng thời, p99 có thể chịu nhiễu nền.

| Case | p50 ms | p95 ms | p99 ms | Event/s từ median | Cost/triệu | CPU/wall | Main alloc median byte | GC count/ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| json-encode | 18.5849 | 27.5963 | 30.4195 | 645685 | 0.00005162 | 1.253 | 34548224 | 10/14 |
| avro-encode | 4.4641 | 7.5286 | 8.4479 | 2688112 | 0.00001240 | 1.619 | 7435376 | 0/0 |
| json-decode | 17.4150 | 23.7360 | 36.9068 | 689061 | 0.00004838 | 1.107 | 31510656 | 6/8 |
| avro-decode | 6.1722 | 9.5136 | 11.8080 | 1944201 | 0.00001715 | 1.404 | 9515392 | 0/0 |

## query

Bối cảnh snapshot: i7-9750H, 12 CPU logic, RAM 34.188.873.728 byte, Windows 11/Adoptium 21.0.12.1, G1, heap 512 MiB. Máy dùng chung; không có nhật ký job nền để xác nhận tác vụ chạy đồng thời, p99 có thể chịu nhiễu nền.

| Case | p50 ms | p95 ms | p99 ms | Event/s từ median | Cost/triệu | CPU/wall | Main alloc median byte | GC count/ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| filter-late | 11.8074 | 12.7315 | 12.9492 | 1016312 | 0.00003280 | 1.370 | 291024 | 0/0 |
| filter-early | 0.4954 | 0.7095 | 0.9557 | 24222850 | 0.00000138 | 2.675 | 291024 | 0/0 |

## cache

Bối cảnh snapshot: i7-9750H, 12 CPU logic, RAM 34.188.873.728 byte, Windows 11/Adoptium 21.0.12.1, G1, heap 512 MiB. Máy dùng chung; không có nhật ký job nền để xác nhận tác vụ chạy đồng thời, p99 có thể chịu nhiễu nền.

| Case | p50 ms | p95 ms | p99 ms | Event/s từ median | Cost/triệu | CPU/wall | Main alloc median byte | GC count/ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| cache-build | 11.8804 | 12.9979 | 13.2308 | 1010067 | 0.00003300 | 1.126 | 439936 | 1/1 |
| cache-read | 0.1199 | 0.2223 | 0.2505 | 100083403 | 0.00000033 | 0.000 | 576024 | 0/0 |
| no-cache | 12.1390 | 12.6209 | 13.3218 | 988549 | 0.00003372 | 0.917 | 576128 | 0/0 |

## cpu

Bối cảnh snapshot: i7-9750H, 12 CPU logic, RAM 34.188.873.728 byte, Windows 11/Adoptium 21.0.12.1, G1, heap 512 MiB. Máy dùng chung; không có nhật ký job nền để xác nhận tác vụ chạy đồng thời, p99 có thể chịu nhiễu nền.

| Case | p50 ms | p95 ms | p99 ms | Event/s từ median | Cost/triệu | CPU/wall | Main alloc median byte | GC count/ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| workers1-parts8-skewfalse | 24.7001 | 27.3475 | 27.8695 | 2429140 | 0.00001372 | 1.190 | 2864 | 0/0 |
| workers2-parts8-skewfalse | 12.8123 | 14.0799 | 14.7240 | 4683000 | 0.00000712 | 2.210 | 2840 | 0/0 |
| workers4-parts8-skewfalse | 7.0561 | 7.4977 | 7.8676 | 8503281 | 0.00000392 | 4.054 | 2840 | 0/0 |
| workers4-parts2-skewfalse | 12.3937 | 13.8361 | 14.4498 | 4841169 | 0.00000689 | 2.043 | 1664 | 0/0 |
| workers4-parts128-skewfalse | 10.0775 | 11.3540 | 12.0314 | 5953858 | 0.00000560 | 3.613 | 26576 | 0/0 |
| workers4-parts8-skewtrue | 20.0935 | 22.7538 | 23.2601 | 2986040 | 0.00001116 | 1.432 | 2920 | 0/0 |

## batch-service

Bối cảnh snapshot: i7-9750H, 12 CPU logic, RAM 34.188.873.728 byte, Windows 11/Adoptium 21.0.12.1, G1, heap 512 MiB. Máy dùng chung; không có nhật ký job nền để xác nhận tác vụ chạy đồng thời, p99 có thể chịu nhiễu nền.

| Case | p50 ms | p95 ms | p99 ms | Event/s từ median | Cost/triệu | CPU/wall | Main alloc median byte | GC count/ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| batch1 | 41.6982 | 44.9384 | 45.2354 | 47964 | 0.00069497 | 1.068 | 30112 | 0/0 |
| batch16 | 3.0011 | 3.1491 | 3.5665 | 666422 | 0.00005002 | 1.488 | 2032 | 0/0 |
| batch128 | 0.7450 | 0.8974 | 0.9169 | 2684564 | 0.00001242 | 2.946 | 400 | 0/0 |

## traps

Bối cảnh snapshot: i7-9750H, 12 CPU logic, RAM 34.188.873.728 byte, Windows 11/Adoptium 21.0.12.1, G1, heap 512 MiB. Máy dùng chung; không có nhật ký job nền để xác nhận tác vụ chạy đồng thời, p99 có thể chịu nhiễu nền.

| Case | p50 ms | p95 ms | p99 ms | Event/s từ median | Cost/triệu | CPU/wall | Main alloc median byte | GC count/ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| discarded-result | 0.0063 | 0.0104 | 0.0104 | 31746031746 | không hợp lệ | 0.000 | 0 | 0/0 |
| consumed-result | 8.4872 | 9.6210 | 9.6826 | 23564898 | 0.00000141 | 1.214 | 24 | 0/0 |

## Event latency (scheduled arrival → completion)

Bối cảnh snapshot: i7-9750H, 12 CPU logic, RAM 34.188.873.728 byte, Windows 11/Adoptium 21.0.12.1, G1, heap 512 MiB. Máy dùng chung; không có nhật ký job nền để xác nhận tác vụ chạy đồng thời, p99 có thể chịu nhiễu nền.

| Cap | Event | p50 ms | p95 ms | p99 ms | Mean ms | Max ms | p99 round min–max ms | Completed/s |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 18000 | 6.3636 | 12.0876 | 13.1805 | 6.3749 | 15.1116 | 12.2978–13.4721 | 9835.98 |
| 16 | 18000 | 8.2704 | 15.4356 | 16.7132 | 8.3258 | 22.9705 | 15.7720–20.9944 | 9598.32 |
| 128 | 18000 | 10.0437 | 17.4379 | 19.0510 | 10.0613 | 22.5882 | 17.9729–20.8882 | 9701.05 |

## Network model (không phải phép đo mạng)

Giả định 50.000.000 byte/s effective bandwidth và 0,05 đơn vị tiền/GiB egress; không nén/header/TLS/retry/fan-out. Làm tròn tổng byte lên.

Bối cảnh snapshot: i7-9750H, 12 CPU logic, RAM 34.188.873.728 byte, Windows 11/Adoptium 21.0.12.1, G1, heap 512 MiB. Máy dùng chung; không có nhật ký job nền để xác nhận tác vụ chạy đồng thời, p99 có thể chịu nhiễu nền.

| Codec | Payload byte | Byte/event | Byte/triệu event | Transfer giây/triệu | Egress cost/triệu |
|---|---:|---:|---:|---:|---:|
| JSONL | 1402760 | 116.8967 | 116896667 | 2.337933 | 0.00544343 |
| Avro raw + count | 845226 | 70.4355 | 70435500 | 1.408710 | 0.00327991 |

## Bộ đếm CPU

Bối cảnh snapshot: i7-9750H, 12 CPU logic, RAM 34.188.873.728 byte, Windows 11/Adoptium 21.0.12.1, G1, heap 512 MiB. Máy dùng chung; không có nhật ký job nền để xác nhận tác vụ chạy đồng thời, p99 có thể chịu nhiễu nền.

API: `com.sun.management.OperatingSystemMXBean.getProcessCpuTime()`. Tính trên delta trong serialization/query/cache/cpu/batch-service/traps CSV. Ước chung lớn nhất của delta dương chỉ mô tả bước quan sát của snapshot; không chứng minh độ phân giải của mọi Windows/JDK.

```text
CPU_COUNTER samples=420 zero=176 minPositiveNs=15625000 gcdPositiveNs=15625000 observedStepMs=15.625
```

