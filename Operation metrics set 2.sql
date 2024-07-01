Create database operation_metrics_2;
show databases;
use operation_metrics_2;

# Table Users

Create table users(
user_id int,
created_at varchar(100),
company_id int,
language varchar(100),
state varchar(100));

Alter table users
Add activated_at varchar(100);

SHOW variables like 'secure_file_priv';

Load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv"
INTO TABLE users
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;


# Events table

user_id	occurred_at	event_type	event_name	location	device	user_type

Create table events(
user_id int,
occured_at date,
event_type varchar(100),
event_name varchar(100),
loaction varchar(100),
device varchar(100),
user_type int);
