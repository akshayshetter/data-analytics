use interview;
select * from new_orders;

Select count(productid) as total_products
from new_orders
where orderdate between '2021-01-01' and '2021-01-31';



select count(productid),extract(month from orderdate) as ordermonth
from new_orders
group by extract(month from orderdate)
having ordermonth = 1;


with cte as (Select userid,orderid,
row_number() over (partition by userid order by orderdate) as rnk
from
new_orders
)
select userid,orderid,rnk
from cte
where rnk = 2;
