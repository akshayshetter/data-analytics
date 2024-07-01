Select s.saleid,c.city,s.amount,c.customerid from sales s
left join customers c on c.customerid = s.customerid
where c.city = 'new york'
group by s.saleid,c.city,c.customerid,s.amount;

SELECT *
FROM Sales
WHERE CustomerID IN (
    SELECT CustomerID
    FROM Customers
    WHERE City = 'New York'
);


select * from customers
where customerid not in
(select distinct customerid from sales)


Select c.customername,s.saleid
from customers c
left join sales s on s.customerid = c.customerid
where c.customername like 'a%'
group by c.customername,s.saleid;

SELECT *
FROM Sales
WHERE EXISTS (
    SELECT *
    FROM Customers
    WHERE Customers.CustomerID = Sales.CustomerID
      AND CustomerName LIKE 'A%'
);

Select * from customers;
Select * from sales;

Select customerid, Sum(amount) as total_amount,
case
when sum(amount) > 600 then "High" 
when sum(amount) between 500 and 600 then "medium" else "Low"
end as customer_category
from sales
group by customerid;

Select customerid, sum(amount) as total_amount
 from sales
 group by customerid;

with current_month_sales as
(
Select sum(amount) as total_amount, monthname(saledate) as month_name,extract(month from saledate) as month_number
from sales
group by monthname(saledate), extract(month from saledate)
),
prev_month_sales as
(
Select month_name,total_amount,
lag(total_amount,1,total_amount) over (order by month_number) as last_month_sales
from current_month_sales 
)
select p.*,
sum(p.total_amount - p.last_month_sales) / p.last_month_sales * 100 as mom_growth
from prev_month_sales p
group by p.month_name,p.total_amount;




