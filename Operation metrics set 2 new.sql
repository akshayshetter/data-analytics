--#1 Weekly User engagement
SELECT 
    EXTRACT(WEEK FROM occured_at) AS Week_Number,
    COUNT(DISTINCT user_id) AS User_Count
FROM
    events
GROUP BY Week_Number;

--#2 User Growth
SELECT Year,week_number, user_count,
	SUM(user_count)OVER (ORDER BY year,week_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_users
FROM
(
SELECT EXTRACT(Year FROM activated_at) as year, EXTRACT(WEEK FROM activated_at) as week_number, COUNT(DISTINCT user_id) as user_count
 FROM users 
 WHERE state = 'active'
 GROUP BY week_number,year
 order by year,week_number
 ) a ;

#3 Weekly retention 

Select EXTRACT(Year from occured_at) as Year, EXTRACT(WEEK from occured_at) as weeknum,
count(distinct user_id) as newuser
from events
where event_name = 'complete_signup'
group by year,weeknum
order by year,weeknum asc;


#4 Weekly enggagemnet

SELECT EXTRACT(Year FROM occured_at) as year, 
EXTRACT(WEEK FROM occured_at) as week_number, device,
COUNT(DISTINCT user_id) as user_count
 FROM events
 WHERE event_type = 'engagement'
 GROUP BY 1,2,3
 order by 1,2,3;
 
 
 Select * from emailevents;
 
 SELECT Year,week_number, user_count,
	SUM(user_count)OVER (ORDER BY year,week_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_email_user
FROM
( SELECT EXTRACT(Year FROM occured_at) as year, 
EXTRACT(WEEK FROM occured_at) as week_number,
COUNT(DISTINCT user_id) as user_count
 FROM emailevents
 group by 1,2
 order by 1,2) a;
 
 Select action,count(*) as num_email from emailevents
 group by 
 action;

Select
100.0*Sum(case when email_action = 'email_open' then 1 else 0 end)/sum(case when email_action = 'email_sent' then 1 else 0 end) as email_open_rate,
100.0*Sum(case when email_action = 'email_clicked' then 1 else 0 end)/sum(case when email_action = 'email_sent' then 1 else 0 end) as email_clicked_rate
from
(Select *,
case when action in ('Sent_weekly_digest','sent_reengagement_email') Then 'email_sent'
when action in ('email_open')
then 'email_open'
When action in ('email_clickthrough')
then 'email_clicked'
end as email_action
from emailevents) a;


SELECT
  100.0 * Sum(CASE WHEN email_action = 'email_open' THEN 1 ELSE 0 END) / Sum(CASE WHEN email_action = 'email_sent' THEN 1 ELSE 0 END) AS email_open_rate,
  100.0 * Sum(CASE WHEN email_action = 'email_clicked' THEN 1 ELSE 0 END) / Sum(CASE WHEN email_action = 'email_sent' THEN 1 ELSE 0 END) AS email_clicked_rate
FROM
(
  SELECT *,
    CASE
      WHEN action IN ('Sent_weekly_digest', 'sent_reengagement_email') THEN 'email_sent'
      WHEN action IN ('email_open') THEN 'email_open'
      WHEN action IN ('email_clickthrough') THEN 'email_clicked'
    END AS email_action
  FROM emailevents
) a;


SELECT action,
       SUM(CASE WHEN action IN ('email_open', 'email_clickthrough') THEN 1 ELSE 0 END) AS Sum
FROM emailevents
GROUP BY action;


SELECT
  COUNT(user_id) AS total_users,
  SUM(CASE WHEN retention_week = 1 THEN 1 ELSE 0 END) AS week_1
FROM
(
  SELECT
    a.user_id,
    a.signup_week,
    b.engagement_week,
    b.engagement_week - a.signup_week AS retention_week
  FROM
    (
      SELECT DISTINCT user_id, EXTRACT(WEEK FROM occured_at) AS signup_week
      FROM events
      WHERE event_type = 'signup_flow'
        AND event_name = 'complete_signup'
        AND EXTRACT(WEEK FROM occured_at) = 18
    ) a
  LEFT JOIN
    (
      SELECT DISTINCT user_id, EXTRACT(WEEK FROM occured_at) AS engagement_week
      FROM events
      WHERE event_type = 'engagement'
    ) b
  ON a.user_id = b.user_id
) AS subquery
GROUP BY user_id;





