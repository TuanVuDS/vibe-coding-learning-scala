-- V1: lược đồ ban đầu. Flyway chạy mỗi file đúng MỘT lần, theo thứ tự version, và ghi lại vào flyway_schema_history.
-- KHÔNG sửa file migration đã chạy ở môi trường khác: hãy thêm V2, V3...

create table users (
  id         bigint generated always as identity primary key,
  email      text        not null,
  name       text        not null check (length(trim(name)) > 0),
  created_at timestamptz not null default now()
);
-- Ràng buộc duy nhất không phân biệt hoa thường: "An@X.vn" và "an@x.vn" là một người.
create unique index users_email_lower_uq on users (lower(email));

create table products (
  id        bigint generated always as identity primary key,
  sku       text   not null unique,
  name      text   not null,
  price_vnd bigint not null check (price_vnd > 0),   -- tiền: số nguyên đơn vị nhỏ nhất, KHÔNG dùng float
  stock     int    not null check (stock >= 0)       -- database là tuyến phòng thủ cuối: tồn kho không bao giờ âm
);

create table orders (
  id         bigint generated always as identity primary key,
  user_id    bigint      not null references users (id),
  status     text        not null default 'PENDING' check (status in ('PENDING', 'PAID', 'SHIPPED', 'CANCELLED')),
  total_vnd  bigint      not null check (total_vnd >= 0),
  created_at timestamptz not null default now()
);
-- PostgreSQL KHÔNG tự tạo index cho cột foreign key. Truy vấn "đơn hàng của một user" cần index này.
create index orders_user_id_idx on orders (user_id);

create table order_items (
  order_id       bigint not null references orders (id) on delete cascade,
  product_id     bigint not null references products (id),
  quantity       int    not null check (quantity > 0),
  unit_price_vnd bigint not null check (unit_price_vnd > 0),  -- chụp lại giá TẠI THỜI ĐIỂM mua
  primary key (order_id, product_id)
);
