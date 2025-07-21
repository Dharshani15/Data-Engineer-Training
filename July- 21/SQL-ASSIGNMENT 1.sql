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

-- 1. Display all employees.
select * from employees;

-- 2. Show only emp_name and salary of all employees.
select emp_name, salary from employees;

-- 3. Find employees with a salary greater than 40,000.
select * from employees where salary > 40000;

-- 4. List employees between age 28 and 32 (inclusive).
select * from employees where age between 28 and 32;

-- 5. Show employees who are not in the HR department.
select * from employees where department != 'HR';

-- 6. Sort employees by salary in descending order.
select * from employees order by salary desc;

-- 7. Count the number of employees in the table
select count(*) as tot_emp from employees;

-- 8. Find the employee with the highest salary.
select * from employees where salary = (select max(salary) from employees);



-- 1. Display employee names along with their department locations (using JOIN).
select e.emp_name, d.location from employees e join departments d on e.department = d.dept_name;

-- 2. List departments and count of employees in each department.
select d.dept_name, count(e.emp_id) as tot_count from employees e left join departments d on d.dept_name = e.department group by d.dept_name;

-- 3. Show average salary per department.
select department, avg(salary) as avg_salary from employees  group by department;

-- 4. Find departments that have no employees (use LEFT JOIN).
select d.dept_name from departments d left join employees e on d.dept_name = e.department where e.emp_id is null;

-- 5. Find total salary paid by each department.
select department, sum(salary) as tot_salary from employees group by department;

-- 6. Display departments with average salary > 45,000.
select department, avg(salary) as avg_salary from employees group by department having avg(salary)>45000;

-- 7. Show employee name and department for those earning more than 50,000
select emp_name, department from employees where salary > 50000;










