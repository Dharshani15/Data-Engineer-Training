create table movies(
movie_id int primary key,
title varchar(100),
genre varchar(100),
release_year int,
rental_rate decimal(10,2)
);

create table customers(
customer_id int primary key,
name varchar(100),
email varchar(100),
city varchar(100)
);

create table rentals(
    rental_id int primary key,
    customer_id int,
    movie_id int,
    rental_date date,
    return_date date,
    foreign key (customer_id) references customers(customer_id),
    foreign key (movie_id) references movies(movie_id)
);

insert into movies values
(1, 'Inception', 'Sci-Fi', 2010, 120.00),
(2, 'The Kashmir Files', 'Drama', 2022, 100.00),
(3, 'Avengers: Endgame', 'Action', 2019, 150.00),
(4, 'Pathaan', 'Thriller', 2023, 130.00),
(5, 'Zootopia', 'Animation', 2016, 90.00);

insert into customers values
(1, 'Amit Sharma', 'amit.sharma@example.com', 'Delhi'),
(2, 'Neha Reddy', 'neha.reddy@example.com', 'Bangalore'),
(3, 'Ravi Verma', 'ravi.verma@example.com', 'Mumbai'),
(4, 'Pooja Nair', 'pooja.nair@example.com', 'Bangalore'),
(5, 'Sunil Mehta', 'sunil.mehta@example.com', 'Chennai');
 
insert into rentals values
(1, 1, 2, '2022-05-10', '2022-05-15'), 
(2, 1, 3, '2023-01-02', null),         
(3, 2, 4, '2024-02-10', '2024-02-12'), 
(4, 3, 2, '2023-03-15', '2023-03-20'), 
(5, 3, 5, '2023-06-10', '2023-06-12'),
(6, 4, 1, '2021-04-22', '2021-04-25'), 
(7, 5, 2, '2022-11-05', '2022-11-08'), 
(8, 2, 1, '2023-12-01', '2023-12-03'); 

-- 1. Retrieve all movies rented by a customer named 'Amit Sharma'.
select m.title from rentals r join customers c on r.customer_id = c.customer_id join movies m on r.movie_id = m.movie_id where c.name = 'Amit Sharma';

-- 2. Show the details of customers from 'Bangalore'.
select * from customers where city = 'Bangalore';

-- 3. List all movies released after the year 2020.
select * from movies where release_year > 2020;

-- 4. Count how many movies each customer has rented.
select c.name, count(r.rental_id) as rentals_count from customers c left join rentals r on c.customer_id = r.customer_id group by c.name;

-- 5. Find the most rented movie title.
select m.title, count (r.rental_id) as times_rented from rentals r join movies m on r.movie_id = m.movie_id group by m.title order by times_rented desc limit 1;

-- 6. Calculate total revenue earned from all rentals.
select sum(m.rental_rate) as total_revenue from rentals r join movies m on r.movie_id = m.movie_id;

-- 7. List all customers who have never rented a movie.
select c.name from customers c left join rentals r on c.customer_id = r.customer_id where r.rental_id is null;

-- 8. Show each genre and the total revenue from that genre.
select m.genre, sum(m.rental_rate) as genre_revenue from rentals r join movies m on r.movie_id = m.movie_id group by m.genre;

-- 9. Find the customer who spent the most money on rentals.
select c.name, sum(m.rental_rate) as total_spent from rentals r join customers c on r.customer_id = c.customer_id join movies m on r.movie_id = m.movie_id group by c.name order by total_spent desc limit 1;

-- 10. Display movie titles that were rented and not yet returned (return_date IS NULL).
select distinct m.title from rentals r join movies m on r.movie_id = m.movie_id where r.return_date is null;




