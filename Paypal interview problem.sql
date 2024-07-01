with cte as(
select department_id, avg(salary) as dept_salary,count(*) as no_of_employee, sum(salary) as total_salary 
from emp
group by department_id
)
select * from 
(select e1.department_id,e1.dept_salary,sum(e2.no_of_employee) no_of_emp,sum(e2.total_salary) as total_salary
,sum(e2.total_salary)/sum(e2.no_of_employee) as company_avg_salary
from cte e1	
inner join cte e2 on e1.department_id != e2.department_id
group by e1.department_id,e1.dept_salary
) A 
where dept_salary < company_avg_salary;

 