create database fitness;
use fitness;
create table Exercises (
  exercise_id int primary key,
  exercise_name varchar(50),
  category varchar(20),
  calories_burn_per_min int
);

create table WorkoutLog (
  log_id int primary key,
  exercise_id int,
  date date,
  duration_min int,
  mood varchar(20),
  foreign key (exercise_id) references Exercises(exercise_id)
);

insert into Exercises values
(1, 'Running', 'Cardio', 10),
(2, 'Cycling', 'Cardio', 8),
(3, 'Weight Lifting', 'Strength', 6),
(4, 'Yoga', 'Flexibility', 4),
(5, 'Jump Rope', 'Cardio', 12),
(6, 'Pilates', 'Flexibility', 5);


insert into WorkoutLog values
(1, 1, '2025-03-05', 30, 'Energized'),
(2, 1, '2024-02-15', 45, 'Tired'),
(3, 2, '2025-03-10', 25, 'Normal'),
(4, 2, '2023-12-12', 40, 'Energized'),
(5, 3, '2025-04-01', 60, 'Tired'),
(6, 3, '2024-02-20', 50, 'Normal'),
(7, 4, '2024-02-10', 30, 'Energized'),
(8, 4, '2025-05-22', 35, 'Normal'),
(9, 5, '2025-03-15', 20, 'Tired'),
(10, 5, '2025-01-10', 15, 'Normal');

-- 1. Show all exercises under the “Cardio” category.
select * from Exercises where category = 'Cardio';

-- 2. Show workouts done in the month of March 2025.
select * from WorkoutLog where month(date) = 3 and year(date) = 2025;

-- 3. Calculate total calories burned per workout (duration × calories_burn_per_min).
select log_id, duration_min * e.calories_burn_per_min as total_calories from WorkoutLog w join Exercises e on  w.exercise_id = e.exercise_id;

-- 4. Calculate average workout duration per category.
select e.category, avg(w.duration_min) as avg_duration from WorkoutLog w join Exercises e on w.exercise_id = e.exercise_id group by e.category;

-- 5. List exercise name, date, duration, and calories burned using a join.
select e.exercise_name, w.date, w.duration_min,(w.duration_min * e.calories_burn_per_min) as total_calories from WorkoutLog w join Exercises e on w.exercise_id = e.exercise_id;

-- 6. Show total calories burned per day.
select w.date, sum(w.duration_min * e.calories_burn_per_min) as total_calories from  WorkoutLog w join Exercises e on w.exercise_id = e.exercise_id group by w.date;

-- 7. Find the exercise that burned the most calories in total.
select exercise_id
from (
  select w.exercise_id, sum(w.duration_min * e.calories_burn_per_min) as total
  from WorkoutLog w join Exercises e on w.exercise_id = e.exercise_id
  group by w.exercise_id
  order by total desc
  limit 1
) as top_exercise;

-- 8. List exercises never logged in the workout log.
select * from Exercises where exercise_id not in  (select distinct  exercise_id from WorkoutLog);

-- 9. Show workouts where mood was “Tired” and duration > 30 mins.
select * from WorkoutLog where mood = 'Tired' and duration_min > 30;

-- 10. Update a workout log to correct a wrongly entered mood.
update WorkoutLog set mood = 'Tired' where log_id = 4;

-- 11. Update the calories per minute for “Running”.
update Exercises set calories_burn_per_min = 11 where exercise_name = 'Running';

-- 12. Delete all logs from Feb 2024
delete from WorkoutLog where month(date) = 2 and year(date) = 2024;



