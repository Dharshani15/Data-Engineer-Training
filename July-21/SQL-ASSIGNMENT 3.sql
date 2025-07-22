create table books(
book_id int primary key,
title varchar(100),
author varchar(100),
genre varchar(100),
price decimal(10,2)
);

create table customers(
customer_id int primary key,
name varchar(100),
email varchar(100),
city varchar(100)
);

create table orders(
order_id int primary key,
customer_id int,
book_id int,
order_date date,
quantity int,
foreign key (customer_id) references customers(customer_id),
foreign key (book_id) references books(book_id)
);

insert into books values
(1, 'The Alchemist', 'Paulo Coelho', 'Fiction', 299.99),
(2, 'Wings of Fire', 'A.P.J. Abdul Kalam', 'Biography', 549.00),
(3, 'Rich Dad Poor Dad', 'Robert Kiyosaki', 'Finance', 650.00),   
(4, 'Python Programming', 'John Zelle', 'Education', 700.00),      
(5, 'The Power of Habit', 'Charles Duhigg', 'Self-Help', 375.00),
(6, 'Deep Learning', 'Ian Goodfellow', 'Education', 800.00);   

insert into customers values
(1, 'Amit Sharma', 'amit@gmail.com', 'Delhi'),
(2, 'Neha Reddy', 'neha@yahoo.com', 'Mumbai'),
(3, 'Ravi Verma', 'ravi@hotmail.com', 'Chennai'),
(4, 'Pooja Mehta', 'pooja@gmail.com', 'Bangalore'),
(5, 'Faizan Ali', 'faizan@gmail.com', 'Hyderabad'),  
(6, 'Meena Iyer', 'meena@gmail.com', 'Hyderabad'); 

insert into orders values
(101, 1, 3, '2021-12-15', 1),   
(102, 2, 1, '2022-07-22', 2),   
(103, 3, 2, '2023-01-10', 1),   
(104, 4, 4, '2023-11-05', 3),   
(105, 5, 5, '2024-03-14', 2),   
(106, 1, 3, '2024-05-20', 1),   
(107, 6, 6, '2022-09-01', 4);  

-- 1. List all books with price above 500.
select * from books where price>500;

-- 2. Show all customers from the city of ‘Hyderabad’.
select * from customers where city= 'Hyderabad';

-- 3. Find all orders placed after ‘2023-01-01’.
select * from orders where order_date > '2023-01-01';

-- 4. Show customer names along with book titles they purchased
select c.name, b.title from customers c join books b on c.customer_id = b.book_id;

-- 5. List each genre and total number of books sold in that genre.
select b.genre, sum(o.quantity) as tot_book_sold from books b join orders o on b.book_id = o.book_id group by b.genre;

-- 6. Find the total sales amount (price × quantity) for each book.
select b.title, sum(b.price * o.quantity) as tot_sales_amount from books b join orders o on b.book_id = o.book_id group by b.title; 

-- 7. Show the customer who placed the highest number of orders.
select c.name, count(o.order_id) as order_count from customers c join orders o on c.customer_id = o.customer_id group by c.name, c.customer_id order by order_count desc limit 1;

-- 8. Display average price of books by genre.
select genre, avg(price) as avg_price from books group by genre;

-- 9. List all books that have not been ordered.
select b.* from books b left join orders o on b.book_id = o.book_id where o.order_id is null;

-- 10. Show the name of the customer who has spent the most in total.
select c.name, sum(b.price * o.quantity) as total_spent from customers c join orders o on c.customer_id = o.customer_id join books b on o.book_id = b.book_id group by   c.customer_id, c.name order by  total_spent desc limit 1;













