create table users (
 id bigint generated always as identity primary key,
 name text not null check (length(name) between 1 and 100),
 email text not null check (length(email) <= 254),
 password_hash text not null,
 role text not null default 'Customer' check (role in ('Customer', 'Admin'))
);
create unique index users_email_uq on users (lower(email));
create table products (
 id bigint generated always as identity primary key,
 sku text not null unique,
 name text not null,
 price bigint not null check (price > 0),
 stock int not null check (stock >= 0),
 active boolean not null default true
);
create table orders (
 id bigint generated always as identity primary key,
 user_id bigint not null references users(id),
 total bigint not null check (total > 0),
 status text not null default 'Pending' check (status in ('Pending','Paid','Shipped','Cancelled'))
);
create index orders_user_idx on orders(user_id,id);
create table order_items (
 order_id bigint not null references orders(id),
 product_id bigint not null references products(id),
 quantity int not null check (quantity between 1 and 1000),
 price bigint not null check (price > 0),
 primary key (order_id, product_id)
);
create table revoked_tokens (jti text primary key, expires_epoch bigint not null);
create index revoked_expiry_idx on revoked_tokens(expires_epoch);
