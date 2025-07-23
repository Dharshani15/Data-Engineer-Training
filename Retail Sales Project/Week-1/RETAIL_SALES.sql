create database retail;
use retail;

create table products (
  product_id int primary key,
  product_name varchar(100),
  category varchar(50),
  price decimal(10, 2)
);

create table stores (
  store_id int primary key,
  store_name varchar(100),
  region varchar(50)
);

create table employees (
  employee_id int primary key,
  name varchar(100),
  role varchar(50),
  store_id int,
  foreign key (store_id) references stores(store_id)
);

create table sales (
  sale_id int primary key,
  product_id int,
  store_id int,
  quantity int,
  sale_date date,
  foreign key (product_id) references products(product_id),
  foreign key (store_id) references stores(store_id)
);

insert into products values
(1, 'Parachute Hair Oil', 'Health & Beauty', 90.00),
(2, 'Dettol Handwash', 'Health & Hygiene', 120.00),
(3, 'Britannia Biscuits', 'Food', 35.00),
(4, 'Amul Butter', 'Dairy', 50.00),
(5, 'Maggi Noodles', 'Food', 15.00),
(6, 'Colgate Toothpaste', 'Health & Hygiene', 40.00),
(7, 'Surf Excel Detergent', 'Cleaning', 180.00);

insert into stores values
(1, 'Big Bazaar Mumbai', 'West'),
(2, 'DMart Bangalore', 'South'),
(3, 'Reliance Fresh Delhi', 'North'),
(4, 'Spencer Kolkata', 'East'),
(5, 'Star Bazaar Pune', 'West'),
(6, 'More Chennai', 'South'),
(7, 'Vishal Mega Mart Lucknow', 'North');

insert into employees values
(101, 'Ravi Kumar', 'Manager', 1),
(102, 'Anita Sharma', 'Cashier', 2),
(103, 'Suresh Mehta', 'Sales Executive', 3),
(104, 'Divya Nair', 'Cashier', 4),
(105, 'Amit Roy', 'Inventory Manager', 5),
(106, 'Pooja Verma', 'Manager', 6),
(107, 'Karan Desai', 'Sales Executive', 7);

insert into sales values
(1001, 1, 1, 10, '2022-03-15'),
(1002, 2, 2, 5, '2023-06-20'),
(1003, 3, 3, 20, '2024-01-05'),
(1004, 4, 4, 15, '2023-11-12'),
(1005, 5, 5, 25, '2022-12-30'),
(1006, 6, 6, 18, '2024-07-10'),
(1007, 7, 7, 22, '2025-04-18'),
(1008, 1, 2, 5, '2025-05-22'),
(1009, 3, 1, 10, '2024-09-14'),
(1010, 5, 3, 30, '2023-02-28'),
(1011, 6, 4, 8, '2022-10-09');

-- Stored Procedure
delimiter //
create procedure GetDailySales(in store int, in date_val date)
begin
  select s.store_id, p.product_name, sum(s.quantity * p.price) as total_sales
  from sales s join products p on s.product_id = p.product_id
  where s.store_id = store and s.sale_date = date_val
  group by p.product_name;
end //
delimiter ;



