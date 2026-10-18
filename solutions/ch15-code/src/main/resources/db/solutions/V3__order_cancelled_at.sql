-- Bài 2: thêm cột mới mà KHÔNG sửa V1/V2. Cột nullable + ràng buộc nhất quán giữa hai cột.
alter table orders add column cancelled_at timestamptz;

-- Đơn ở trạng thái CANCELLED khi và chỉ khi có cancelled_at.
-- Dữ liệu cũ không có đơn CANCELLED nào nên ràng buộc hợp lệ ngay. Trên bảng lớn có dữ liệu sẵn, cân nhắc
-- `add constraint ... not valid` rồi `validate constraint` để tránh khóa bảng lâu.
alter table orders add constraint orders_cancelled_consistent
  check ((status = 'CANCELLED') = (cancelled_at is not null));

-- Bài 6: "10 đơn mới nhất của một user" — index khớp cả điều kiện lọc lẫn thứ tự sắp xếp.
create index orders_user_created_idx on orders (user_id, created_at desc, id desc);
