create table employees (
emp_id int primary key,
emp_name varchar(100),
department varchar(50),
salary int,
age int
);

insert into employees values
(101, 'Amit Sharma', 'Engineering', 60000, 30),
(102, 'Neha Reddy', 'Marketing', 45000, 28),
(103, 'Faizan Ali', 'Engineering', 58000, 32),
(104, 'Divya Mehta', 'HR', 40000, 29),
(105, 'Ravi Verma', 'Sales', 35000, 26);

create table departments (
dept_id int primary key,
dept_name varchar(50),
location varchar(50)
);

insert into departments values
(1, 'Engineering', 'Bangalore'),
(2, 'Marketing', 'Mumbai'),
(3, 'HR', 'Delhi'),
(4, 'Sales', 'Chennai');

select * from employees;
select emp_name, salary from employees;
select * from employees where salary > 40000;
select * from employees where age between 28 and 32;
select * from employees where department != 'HR';
select * from employees order by salary desc;
select count(*) as tot_emp from employees;
select * from employees where salary = (select max(salary) from employees);

select e.emp_name, d.location from employees e join departments d on e.department = d.dept_name;
select d.dept_name, count(e.emp_id) as tot_count from employees e left join departments d on d.dept_name = e.department group by d.dept_name;
select department, avg(salary) as avg_salary from employees  group by department;
select d.dept_name from departments d left join employees e on d.dept_name = e.department where e.emp_id is null;
select department, sum(salary) as tot_salary from employees group by department;
select department, avg(salary) as avg_salary from employees group by department having avg(salary)>45000;
select emp_name, department from employees where salary > 50000;





