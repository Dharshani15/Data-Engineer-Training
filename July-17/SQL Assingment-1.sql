create table products(
product_id int primary key,
product_name varchar(100),
category varchar(100),
price decimal(10,2),
stock_quantity int,
added_date date);

insert into products values
(1, 'Wireless Headphones', 'Electronics', 2999.00, 50, '2022-07-17'),
(2, 'Mouse', 'Accessories', 1499.50, 30, '2021-03-09'),
(3, 'Laptop - Dell ', 'Computers', 559.99, 9, '2025-12-31'),
(4, 'Smartphone - Samsung Galaxy', 'Electronics', 2229.00, 25, '2023-03-15'),
(5, 'Dining Table', 'Furniture', 399.95, 5, '2021-06-07');

select * from products;
select product_name, price from products;
select * from products where stock_quantity<10;
select *from products where price between 500 and 2000;
select * from products where added_date >2023-01-01;
select*from products where product_name like 'S%';
select * from products where category in ('Electronics','Furniture');

update products set price = 1407.12 where product_id=3;
update products set stock_quantity = stock_quantity+5 where category ='Electronics';
delete from products where product_id=5;
delete from products where stock_quantity=0;


