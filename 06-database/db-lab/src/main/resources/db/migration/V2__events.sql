-- V2: bảng sự kiện dùng cho thí nghiệm index và query plan (mục 3.7 của Chương 15).
-- Chủ ý CHƯA có index trên (user_id, created_at): bài học sẽ đo plan trước và sau khi thêm.
create table page_views (
  id         bigint generated always as identity primary key,
  user_id    bigint      not null,
  path       text        not null,
  created_at timestamptz not null
);
