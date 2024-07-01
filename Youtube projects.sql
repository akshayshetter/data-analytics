use projects;
select * from menu;
#1 Total amount spent by customer
	
select s.customer_id,sum(m.price) as total_amount from menu m
right join sales s on m.product_id = s.product_id
group by s.customer_id
order by total_amount desc;


#2 How many days customer has visited restaurant

Select customer_id, count(distinct order_date) as visited_count from sales
group by customer_id;

#3 frst order ordered by each customer

with cte as (Select a.customer_id, a.product_name, row_number() over ( partition by a.customer_id order by a.order_date) as rnk
from
(Select s.customer_id,m.product_name,s.order_date from sales s
join menu m on s.product_id = m.product_id) a
)
Select customer_id, product_name,rnk
from cte
where rnk = 1
group by customer_id, product_name;


with cte as 
(Select s.customer_id, m. product_name, row_number() over (partition by s.customer_id order by s.order_date) as rnk
 from sales s
join menu m on s.product_id = m.product_id)
select customer_id, product_name, rnk
from cte
where rnk = 1;


#4 most purchased item on the menu

Select s.product_id, m.product_name,count(m.product_name) as max_count from sales s
join menu m on s.product_id = m.product_id
group by m.product_name, s.product_id
order by max_count desc
limit 1;

#5 which item was most purchased by each customer

with cte as
(Select s.customer_id,s.product_id,m.product_name,count(m.product_name) as most_prchased, row_number() over(partition by s.customer_id order by count(m.product_name) desc) as rnk from sales s
join menu m on s.product_id = m.product_id
group by s.customer_id,s.product_id,m.product_name)
select customer_id, product_name, rnk, product_id, most_prchased
from cte
where rnk = 1;

#6 first order purchased by customer after joining
with cte as
(Select s.customer_id, s.product_id,m.product_name,s.order_date,e.join_date, 
row_number() over (partition by s.customer_id order by s.order_date) as rnk from sales s
join menu m on s.product_id = m.product_id
join members e on s.customer_id = e.customer_id
where s.order_date >= e.join_date
group by s.customer_id,s.product_id,m.product_name,s.order_date,e.join_date
order by e.join_date desc)
select customer_id, product_name, order_date, join_date
from cte
where rnk= 1
;

#7 item purchased before customer become a member

with cte as
(Select s.customer_id, s.product_id,m.product_name,s.order_date,e.join_date, 
row_number() over (partition by s.customer_id order by s.order_date) as rnk from sales s
join menu m on s.product_id = m.product_id
join members e on s.customer_id = e.customer_id
where s.order_date <= e.join_date
group by s.customer_id,s.product_id,m.product_name,s.order_date,e.join_date
order by e.join_date desc)
select customer_id, product_name, order_date, join_date
from cte
where rnk= 1
;

