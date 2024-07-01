drop database companydb;

create database companydb;

use companydb;

create table employee(
  fname varchar(30),
  minit char(1),
  lname varchar(30),
  ssn char(9),
  bdate date,
  address varchar(30),
  sex char(1),
  salary float(10, 2),
  super_ssn char(9),
  dno smallint(6),
  constraint pk_employee PRIMARY KEY (ssn)
);

create table department(
  dname varchar(30),
  dnumber smallint primary key,
  mgr_ssn char(9),
  mgr_start_date date
);

create table dept_locations(
  dnumber smallint,
  dlocation varchar(20),
  constraint composite_pk_dept_loc PRIMARY KEY (dnumber, dlocation)
);
create table project(
  pname varchar(30),
  pnumber smallint,
  plocation varchar(30),
  dnum smallint,
  constraint pk_project PRIMARY KEY (pnumber)
);

create table works_on(
  essn char(9),
  pno smallint,
  hours float(4, 2),
  constraint pk_works_on PRIMARY KEY (essn, pno)
);

create table dependent(
  essn char(9),
  dependent_name varchar(30),
  sex char(1),
  bdate date,
  relationship varchar(20),
  constraint pk_dependent PRIMARY KEY (essn, dependent_name)
);
insert into employee(
    fname,
    minit,
    lname,
    ssn,
    bdate,
    address,
    sex,
    salary,
    super_ssn,
    dno
  )
VALUES
  (
    'John',
    'B',
    'Smith',
    '123456789',
    '1965-01-09',
    '731 Fondren, Houston, TX',
    'M',
    30000,
    '333445555',
    5
  ),
  (
    'Franklin',
    'T',
    'Wong',
    '333445555',
    '1955-01-09',
    '638 Fondren, Houston, TX',
    'M',
    40000,
    '888665555',
    5
  ),
  (
    'Alicia',
    'J',
    'Zelaya',
    '999887777',
    '1968-01-09',
    '3321 Fondren, Houston, TX',
    'F',
    25000,
    '987654321',
    4
  ),
  (
    'Jennifer',
    'S',
    'Wallace',
    '987654321',
    '1941-01-09',
    '21 Fondren, Houston, TX',
    'F',
    43000,
    '888665555',
    4
  ),
  (
    'Ramesh',
    'K',
    'Narayan',
    '666884444',
    '1962-01-09',
    '975 Fondren, Houston, TX',
    'M',
    38000,
    '333445555',
    5
  ),
  (
    'Joyce',
    'A',
    'English',
    '453453453',
    '1972-01-09',
    '5631 Fondren, Houston, TX',
    'F',
    25000,
    '333445555',
    5
  ),
  (
    'Ahmad',
    'V',
    'Jabbar',
    '987987987',
    '1969-01-09',
    '980 Fondren, Houston, TX',
    'M',
    25000,
    '987654321',
    4
  ),
  (
    'James',
    'E',
    'Borg',
    '888665555',
    '1937-01-09',
    '450 Fondren, Houston, TX',
    'M',
    55000,
    NULL,
    1
  );

select * from employee;

insert into department(dname, dnumber, mgr_ssn, mgr_start_date)
VALUES
  ('Research', 5, 333445555, '1988-05-22'),
  ('Administration', 4, 987654321, '1995-05-22'),
  ('Headquarters', 1, 888665555, '1981-05-22');

insert into dept_locations(dnumber, dlocation)
values
  (1, 'Houston'),
  (4, 'Stafford'),
  (5, 'Bellaire'),
  (5, 'Sugarland'),
  (5, 'Houston');

insert into works_on (essn, pno, hours)
values
  ('123456789', 1, 32.5),
  ('123456789', 2, 7.5),
  ('666884444', 3, 40.0),
  ('453453453', 1, 20.0),
  ('453453453', 2, 20.0),
  ('333445555', 2, 10.0),
  ('333445555', 3, 10.0),
  ('333445555', 10, 10.0),
  ('333445555', 20, 10.0),
  ('999887777', 30, 30.0),
  ('999887777', 10, 10.0),
  ('987987987', 10, 35.0),
  ('987987987', 30, 5.0),
  ('987654321', 30, 20.0),
  ('987654321', 20, 15.0),
  ('888665555', 20, NULL);

insert into project(pname, pnumber, plocation, dnum)
values
  ('ProductX', 1, 'Bellaire', 5),
  ('ProductY', 2, 'Sugarland', 5),
  ('ProductZ', 3, 'Houston', 5),
  ('Computerization', 10, 'Stafford', 4),
  ('Reorganization', 20, 'Houston', 1),
  ('Newbenefits', 30, 'Stafford', 4);

insert into dependent(essn, dependent_name, sex, bdate, relationship)
values
  (
    '333445555',
    'Alice',
    'F',
    '1986-04-05',
    'Daughter'
  ),
  (
    '333445555',
    'Theodore',
    'M',
    '1983-04-05',
    'Son'
  ),
  ('333445555', 'Joy', 'F', '1958-04-05', 'Spouse'),
  (
    '987654321',
    'Abner',
    'M',
    '1942-04-05',
    'Spouse'
  ),
  ('123456789', 'Michael', 'M', '1988-04-05', 'Son'),
  (
    '123456789',
    'Alice',
    'M',
    '1988-04-05',
    'Daughter'
  ),
  (
    '123456789',
    'Elizabeth',
    'M',
    '1967-04-05',
    'Spouse'
  );
# use of Alias  
Select Sum(salary) AS Total_salary from employee;


# use of inner join

Select works_on.hours, employee.fname
from works_on
inner join employee on works_on.essn = employee.ssn;

# use of left join
Select fname, lname, sex, dname,mgr_ssn from employee
right join department
on employee.dno=department.dnumber;


#use of count function

Select count(*) from employee;

Select * from employee;

Select count(salary) from employee where salary<35000;

Select * from employee where super_ssn is null;


# use of sum function

Select sum(salary) from employee;

select sum(salary) from employee
where sex='m' ;

#use if max function

Select max(salary) as Maximum_Salary from employee
where sex='m';

#use of min function

Select min(salary) as minimum_salary from employee
where sex='f';	

# Use of AVG function manually

Select sum(Salary)/Count(Salary)
from employee ;

# Use of Avg function

Select Max(Salary) as max_salary,Min(Salary) as min_salary, Avg(salary) as Avg_salary from employee;

	#use of group by function
    
    Select sex,avg(salary) as avg_salary
    from employee
    group by sex;
    
    
    Select essn,sum(hours) as work_hours
    from works_on
    where hours is NOT NULL
    group by essn;
    
    
    #Calculate average slary for male emolyee in each department
    
    Select dno,sex, avg(salary) as avg_salary
    from Employee where sex="m"
    Group by dno;
    
	Select dno,sex, avg(salary) as avg_salary
    from Employee where sex="f"
    Group by dno
    Order by avg_salary desc;
    
    #Use of distinct function;
    
	Select distinct super_ssn from employee
    where super_ssn is not null;
    
    #count number of manager ids
    
    Select count(distinct super_ssn) from employee
    where super_ssn is not null;
    
    
    Select dno, avg(salary) as avg_salary from employee
    group by dno
    having avg_salary>32000;
    
    
    #use of extract function
    
    
    Select *
    from employee;
    
    Select extract(month from bdate) as year_of_birth
    from employee
    group by year_of_birth;
    
    Select ssn, Salary,
    case
    when salary < 35000 then"less"
    when Salary = 35000 then "equal"
    else " more" 
    end as Pay_scale
     from employee 
     order by salary;
     
     
     # Use of nested query
    Select ssn,fname,lname,dno
    from employee
    where ssn in(
     Select distinct super_ssn
     from employee
     where super_ssn is not null);
     
# use of inline sub query

Select min(avg_salary),max(avg_salary)
from (
select avg(salary) as avg_salary,dno
from employee
group by dno) as dept_avg_salary;

# Use of Scalar sub query

Select ssn,fname,salary
from employee
where salary > (
select avg(salary) as avg_salary from employee);

#Use of CTE

With employee_hours as(
Select essn,sum(hours) as total_work_hours
from works_on
group by essn
)

select ssn,fname,dno,total_work_hours
from employee
inner join employee_hours
on employee.ssn=employee_hours.essn
where total_work_hours is not null;


With employee_hours as(
Select essn,sum(hours) as total_work_hours
from works_on
group by essn
),
employee_avg_salary as(
select ssn,avg(salary) as avg_salary
from employee
group by ssn
)

Select * from employee_avg_salary
inner join(
select ssn,fname,dno,total_work_hours
from employee
inner join employee_hours
on employee.ssn=employee_hours.essn
) as inner_query
on employee_avg_salary.ssn=inner_query.ssn
where total_work_hours is not null;


