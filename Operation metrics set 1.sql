Create database operation_metrics;
show databases;
use operation_metrics;
# table-1 Job_data

create table jobdata(
ds varchar(100),
job_id int,
acotor_id int,
event varchar(100),
language varchar(100),
time_spent int,
org varchar(100));

SHOW variables like 'secure_file_priv';

Load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/job_data (1).csv"
INTO TABLE jobdata
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;


# number of jobs revived per day in Nov
select * from jobdata;

Select count(distinct job_id)/(30*24)
from 
jobdata
where 
ds Between '01-11-2020' and '30-11-2020';


# Calculate 7 day rolling average

select 
ds, jobs_revived,
avg(jobs_revived) over(Order by ds rows between 6 preceding and current row) 
as throughput_7
from
(
Select ds, 
count(distinct job_id) as jobs_revived
from jobdata
group by ds
order by ds
)
as a;

# Percentage share of each language
Select 
language,
num_jobs,
100*(num_jobs/total_jobs) as Pct_jobs
from
(Select language,count(distinct job_id) as num_jobs
from jobdata
group by language)as a
cross join 
(Select count(distinct job_id) As total_jobs from jobdata)as  b ;


# Duplicate row in data

Select*from
(Select *,row_number()over(partition by job_id) as row_num
from jobdata) as a
where 
row_num>1;







