select * from sales;

with cte as(select productid,category,SalesAmount, salesdate,
avg(SalesAmount) over (order by salesamount desc) as avgsales
from sales
order by salesdate)
select productid,salesamount
from cte 
where salesamount <= avgsales
group by productid,salesamount;


select a.productid, a.category,a.salesamount,a.avgsales
from
(select productid,category,SalesAmount, salesdate,
avg(SalesAmount) over (order by salesamount desc) as avgsales
from sales
order by salesdate) a
where a.salesamount < a.avgsales;

