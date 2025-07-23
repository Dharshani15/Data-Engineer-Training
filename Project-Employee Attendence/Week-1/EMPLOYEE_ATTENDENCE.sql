create database EmployeeDB;
use EmployeeDB;

create table employees (
    employee_id int primary key,
    first_name varchar(50),
    last_name varchar(50),
    department varchar(50),
    designation varchar(50)
);

create table attendance (
    attendance_id int primary key,
    employee_id int,
    attendance_date date,
    clock_in time,
    clock_out time,
    foreign key (employee_id) references employees(employee_id)
);

create table tasks (
    task_id int primary key,
    employee_id int,
    task_description varchar(100),
    task_date date,
    status varchar(20),
    foreign key (employee_id) references employees(employee_id)
);

insert into employees values
(101, 'John', 'Doe', 'HR', 'Manager'),
(102, 'Jane', 'Smith', 'IT', 'Developer'),
(103, 'Alice', 'Brown', 'Finance', 'Analyst'),
(104, 'Bob', 'Johnson', 'Marketing', 'Executive'),
(105, 'Charlie', 'Davis', 'IT', 'Support'),
(106, 'Eve', 'Miller', 'HR', 'Recruiter'),
(107, 'Frank', 'Wilson', 'Finance', 'Accountant');

insert into attendance values
(1001, 101, '2023-07-01', '09:00:00', '17:00:00'),
(1002, 102, '2023-07-01', '09:15:00', '17:15:00'),
(1003, 103, '2024-01-15', '08:45:00', '16:45:00'),
(1004, 104, '2024-01-15', '09:10:00', '17:00:00'),
(1005, 105, '2025-03-10', '09:00:00', '18:00:00'),
(1006, 106, '2025-03-10', '09:30:00', '17:30:00'),
(1007, 107, '2023-12-05', '08:50:00', '16:50:00');

insert into tasks values
(5001, 102, 'Fix server downtime issue', '2024-01-16', 'Completed'),
(5002, 103, 'Prepare financial report', '2024-01-16', 'In Progress'),
(5003, 104, 'Design marketing campaign', '2024-01-17', 'Completed'),
(5004, 105, 'Setup new workstations', '2025-03-11', 'Pending'),
(5005, 101, 'Conduct employee training', '2023-07-02', 'Completed'),
(5006, 106, 'Screen candidates', '2025-03-12', 'In Progress'),
(5007, 107, 'Update payroll data', '2023-12-06', 'Completed');

-- Stored Procedure
Delimiter // 
create procedure GetTotalWorkingHours(in emp_id int)
begin
select e.first_name,e.last_name,
SUM(TIMEDIFF(clock_out, clock_in)) AS total_working_hours
from attendance a join employees e on a.employee_id = e.employee_id
where a.employee_id = emp_id and clock_out is not null
group by e.first_name, e.last_name;
end //
Delimiter ;





















