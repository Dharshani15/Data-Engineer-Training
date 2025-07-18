create table departments(
dept_id int primary key,
dept_name varchar(100)
);
insert into departments values
(1, 'Human Resources'),
(2, 'Engineering'),
(3, 'Marketing');

create table employees(
emp_id int primary key,
emp_name varchar(100),
dept_id int,
salary int);
insert into employees values
(101, 'Amit Sharma', 1, 36000),
(102, 'Neha Reddy', 2, 45000),
(103, 'Faizan Ali', 2, 48000),
(104,'Divya Mehta',3,35000),
(105, 'Ravi verma', NULL, 28000);

select e.emp_name, d.dept_name from   departments d inner join employees e on e.dept_id = d.dept_id;
select e.emp_name from employees e left  join departments d on e.dept_id = d.dept_id where d.dept_id is null;
select d.dept_name, count(d.dept_id) as tot_members from employees e join departments d on e.dept_id = d.dept_id group by dept_name;
select d.dept_name  from employees e right join  departments d on e.dept_id = d.dept_id where e.emp_id is null;
select e.emp_name, d.dept_name from employees e join departments d on e.dept_id=d.dept_id where e.salary>40000;







