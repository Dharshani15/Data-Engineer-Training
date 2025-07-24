create database travel;
use travel;

create table Destinations (
  destination_id int primary key,
  city varchar(50),
  country varchar(50),
  category varchar(20), 
  avg_cost_per_day int
);

create table Trips (
  trip_id int primary key,
  destination_id int,
  traveler_name varchar(50),
  start_date date,
  end_date date,
  budget int,
  foreign key (destination_id) references Destinations(destination_id)
);
insert into Destinations values
(1, 'Goa', 'India', 'Beach', 2500),
(2, 'Jaipur', 'India', 'Historical', 1800),
(3, 'Paris', 'France', 'Historical', 5500),
(4, 'Cape Town', 'South Africa', 'Adventure', 3200),
(5, 'Banff', 'Canada', 'Nature', 4000),
(6, 'Bali', 'Indonesia', 'Beach', 2700),
(7, 'Kyoto', 'Japan', 'Historical', 3800); 

insert into Trips values
(101, 1, 'Amit Sharma', '2025-03-01', '2025-03-05', 15000),
(102, 2, 'Sneha Verma', '2024-11-10', '2024-11-18', 20000),
(103, 3, 'John Smith', '2023-07-01', '2023-07-10', 60000),
(104, 4, 'Lerato Mokoena', '2022-12-01', '2022-12-05', 18000),
(105, 5, 'Alex Chen', '2023-06-15', '2023-06-23', 35000),
(106, 6, 'Meera Nair', '2024-05-01', '2024-05-03', 10000),
(107, 1, 'Rahul Roy', '2023-10-20', '2023-10-27', 20000),
(108, 3, 'Sophia Brown', '2025-01-15', '2025-01-20', 30000),
(109, 5, 'Carlos Garcia', '2024-03-01', '2024-03-10', 40000),
(110, 2, 'Neha Singh', '2025-04-05', '2025-04-09', 18000),
(111, 4, 'David Miller', '2023-02-01', '2023-02-09', 28000);

-- 1. Show all trips to destinations in “India”.
select * from Trips t join Destinations d on t.destination_id = d.destination_id where d.country = 'India';

-- 2. List all destinations with an average cost below 3000.
select * from  Destinations where avg_cost_per_day < 3000;

-- 3. Calculate the number of days for each trip.
select trip_id, datediff(end_date, start_date) + 1 as trip_duration_days from Trips;

-- 4. List all trips that last more than 7 days.
select * from Trips where datediff(end_date, start_date) + 1 > 7;

-- 5. List traveler name, destination city, and total trip cost (duration × avg_cost_per_day).
select t.traveler_name, d.city, (datediff(t.end_date, t.start_date) + 1) * d.avg_cost_per_day as total_trip_cost from Trips t join Destinations d on t.destination_id = d.destination_id;

-- 6. Find the total number of trips per country.
select d.country, count(*) AS total_trips from Trips t join Destinations d on t.destination_id = d.destination_id group by  d.country;

-- 7. Show average budget per country.
select d.country, avg(t.budget) as avg_budget from Trips t join Destinations d on t.destination_id = d.destination_id group by d.country;

-- 8. Find which traveler has taken the most trips.
SELECT traveler_name from Trips group by traveler_name order by count(*) desc limit 1;

-- 9. Show destinations that haven’t been visited yet.
select * from Destinations where destination_id not in (select distinct destination_id from Trips);

-- 10. Find the trip with the highest cost per day.
select *, (budget / (datediff(end_date, start_date) + 1)) as cost_per_day from Trips order by  cost_per_day desc limit 1;

-- 11. Update the budget for a trip that was extended by 3 days.
update Trips set budget = budget + (3 * (select avg_cost_per_day from Destinations where destination_id = 1)), end_date = date_add(end_date, interval 3 day) where trip_id = 101;

-- 12. Delete all trips that were completed before jan 1 2023
delete from Trips where end_date < '2023-01-01';


