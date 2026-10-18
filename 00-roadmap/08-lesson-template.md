# Template bắt buộc cho mỗi bài học

Mọi chương trong giáo trình tuân theo cấu trúc dưới đây. Người biên soạn chương mới hãy sao chép
khung này và điền đầy đủ; **không để mục nào chỉ có tiêu đề**.

```markdown
# Chương N — Tên chương

> Phiên bản: Scala x.y.z · JDK nn · Dependency: ... · Trạng thái kiểm chứng: ...

## 1. Mục tiêu học tập
## 2. Kiến thức tiên quyết
## 3. Lý thuyết
   Với mỗi chủ đề quan trọng:
   - Vấn đề cần giải quyết
   - Vì sao cần
   - Cách hoạt động
   - Lựa chọn thay thế
   - Ưu / nhược điểm
   - Khi nào nên dùng
   - Lỗi thiết kế thường gặp
## 4. Thuật ngữ tiếng Anh
## 5. Ví dụ đơn giản
## 6. Ví dụ thực tế (BE và/hoặc DE)
## 7. Code đầy đủ  (đường dẫn tới file trong thư mục code/)
## 8. Giải thích code
## 9. Output mong đợi
## 10. Lỗi thường gặp  (kèm thông báo lỗi THẬT từ compiler/runtime)
## 11. Cách debug
## 12. Bài tập (cơ bản → nâng cao)
## 13. Đáp án và lời giải  (đặt trong solutions/, bài học chỉ dẫn link)
## 14. Câu hỏi kiểm tra
## 15. Mini project
## 16. Nguồn tài liệu chính thức
## 17. Checklist tự đánh giá
```

## Quy ước trạng thái kiểm chứng

| Nhãn | Ý nghĩa |
|---|---|
| `Đã biên dịch & chạy` | Code đã được compile bằng đúng phiên bản ghi trong bài và chạy cho ra output được trích dẫn |
| `Đã kiểm tra logic test` | Test đã chạy, nhưng với bản mô phỏng API của framework test (ví dụ môi trường biên soạn không tải được thư viện) |
| `Chưa kiểm chứng` | Code được viết theo tài liệu chính thức nhưng chưa chạy; người học cần chạy và báo lỗi nếu có |
