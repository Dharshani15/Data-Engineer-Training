create table students (
student_id int primary key,
name varchar(100),
age int,
gender varchar(10),
department_id int
);

create table departments (
department_id int primary key,
department_name varchar(100),
head_of_department varchar(100) 
);

create table courses (
course_id int primary key,
course_name varchar(100),
department_id int,
credit_hours int
);

insert into students values
(1, 'Amit Sharma', 20, 'Male', 1),
(2, 'Neha Reddy', 22, 'Female', 2),
(3, 'Faizan Ali', 21, 'Male', 1),
(4, 'Divya Mehta', 23, 'Female', 3),
(5, 'Ravi Verma', 22, 'Male', 2);

insert into departments values
(1, 'Computer Science', 'Dr. Rao'),
(2, 'Electronics', 'Dr. Iyer'),
(3, 'Mechanical', 'Dr. Khan');

insert into  courses values
(101, 'Data Structures', 1, 4),
(102, 'Circuits', 2, 3),
(103, 'Thermodynamics', 3, 4),
(104, 'Algorithms', 1, 3),
(105, 'Microcontrollers', 2, 2);

-- 1. List all students with name, age, and gender.
select name, age,gender from students;

-- 2. Show names of female students only.
select name from students where gender='Female';

-- 3. Display all courses offered by the Electronics department.
select c.course_name from courses c join departments d on c.department_id = d.department_id where d.department_name='Electronics';

-- 4. Show the department name and head for department_id = 1.
select department_name , head_of_department from departments where department_id=1;

-- 5. Display students older than 21 years.
select name,age from students where age>21;

-- 6. Show student names along with their department names.
select s.name, d.department_name from students s join departments d on s.department_id = d.department_id;

-- 7. List all departments with number of students in each.
select d.department_name, count(s.student_id) as no_of_students from departments d left join students s on d.department_id = s.department_id group by d.department_name;

-- 8. Find the average age of students per department.
select d.department_name, avg(s.age) as avg_age from departments d join students s on d.department_id = s.department_id group by department_name;

-- 9. Show all courses with their respective department names.
select c.course_name, d.department_name from courses c left join departments d on c.department_id = d.department_id;

-- 10. List departments that have no students enrolled
select d.department_name from departments d left join students s on d.department_id = s.department_id where s.student_id is null;

-- 11. Show the department that has the highest number of courses.
select d.department_name, count(c.course_id) as tot_course from departments d join courses c on d.department_id = c.department_id group by department_name order by tot_course desc limit 1;

-- 12. Find names of students whose age is above the average age of all students.
select name, age from students where age>(select avg(age) from students);

-- 13. Show all departments that offer courses with more than 3 credit hours.
select distinct d.department_name,c.credit_hours from departments d  join courses c where c.credit_hours >3;

-- 14. Display names of students who are enrolled in the department with the fewest courses.
select s.name from students s where s.department_id=(select c.department_id from courses c group by c.department_id order by count(c.course_id) asc limit 1);

-- 15. List the names of students in departments where the HOD's name contains 'Dr.'.
select s.name from students s join departments d on s.department_id = d.department_id where head_of_department like '%Dr.%';

-- 16. Find the second oldest student (use subquery or LIMIT/OFFSET method).
select name, age from students order by age desc limit 2 offset 1;

-- 17. Show all courses that belong to departments with more than 2 student
select c.course_name from courses c join students s on c.department_id = s.department_id group by c.course_id, c.course_name, c.department_id having count(s.student_id)>2;






