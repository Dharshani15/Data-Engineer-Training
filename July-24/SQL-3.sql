create database pet;
use pet;
CREATE TABLE Pets (
pet_id INT PRIMARY KEY,
name VARCHAR(50),
type VARCHAR(20),
breed VARCHAR(50),
age INT,
owner_name VARCHAR(50)
);
INSERT INTO Pets VALUES
(1, 'Buddy', 'Dog', 'Golden Retriever', 5, 'Ayesha'),
(2, 'Mittens', 'Cat', 'Persian', 3, 'Rahul'),
(3, 'Rocky', 'Dog', 'Bulldog', 6, 'Sneha'),
(4, 'Whiskers', 'Cat', 'Siamese', 2, 'John'),
(5, 'Coco', 'Parrot', 'Macaw', 4, 'Divya'),
(6, 'Shadow', 'Dog', 'Labrador', 8, 'Karan');

CREATE TABLE Visits (
visit_id INT PRIMARY KEY,
pet_id INT,
visit_date DATE,
issue VARCHAR(100),
fee DECIMAL(8,2),
FOREIGN KEY (pet_id) REFERENCES Pets(pet_id)
);
INSERT INTO Visits VALUES
(101, 1, '2024-01-15', 'Regular Checkup', 500.00),
(102, 2, '2024-02-10', 'Fever', 750.00),
(103, 3, '2024-03-01', 'Vaccination', 1200.00),
(104, 4, '2024-03-10', 'Injury', 1800.00),
(105, 5, '2024-04-05', 'Beak trimming', 300.00),
(106, 6, '2024-05-20', 'Dental Cleaning', 950.00),
(107, 1, '2024-06-10', 'Ear Infection', 600.000);

-- 1. List all pets who are dogs.
select * from Pets where type = 'Dog';

-- 2. Show all visit records with a fee above 800.
select * from Visits where fee > 800;

-- 3. List pet name, type, and their visit per pet.
select p.name as pet_name, p.type, v.visit_date, v.issue, v.fee from Pets p join Visits v on p.pet_id = v.pet_id;

-- 4. Show the total number of visits per pet.
select p.name as pet_name, count(v.visit_id) as total_visits from Pets p join Visits v on p.pet_id = v.pet_id group by p.name;

-- 5. Find the total revenue collected from all visits.
select sum(fee) AS total_revenue from Visits;

-- 6. Show the average age of pets by type.
select type, avg(age) as avg_age from Pets group by  type;

-- 7. List all visits made in the month of March.
select * from Visits where month(visit_date) = 3;

-- 8. Show pet names who visited more than once.
select p.name from Pets p join Visits v on p.pet_id = v.pet_id group by p.pet_id, p.name having count(v.visit_id) > 1;

-- 9. Show the pet(s) who had the costliest visit.
select p.name, v.fee from Visits v join Pets p on p.pet_id = v.pet_id where v.fee = (select max(fee) from Visits);

-- 10. List pets who haven’t visited the clinic yet.
select * from Pets where pet_id not in (select distinct pet_id from Visits);

-- 11. Update the fee for visit_id 105 to 350.
update Visits set fee = 350.00 where visit_id = 105;

-- 12. Delete all visits made before Feb 2024.
delete from Visits where visit_date < '2024-02-01';

