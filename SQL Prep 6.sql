Select department,avg(salary) as dept_avg from new_employees
group by department;

Select orderid,orderdate,totalamount,extract(month from orderdate) as month
from new_orders
where extract(month from orderdate) = 1;

Select * from new_employees;


Select n.category,sum(o.totalamount) as total_sales from new_products n
left join new_orders o on o.productid = n.productid
group by n.category
order by total_sales;

select * from new_employees;

Select employeeid,firstname,salary from
new_employees
order by salary desc
limit 1 ;

SELECT *
FROM new_Employees
WHERE Salary = (SELECT MAX(Salary) FROM new_Employees);


Select orderid, sum(quantity) as total_quantity,sum(totalamount) as total_sum
from new_orders
group by orderid;


Select * from new_employees;
select * from sales;

with cte as(select e.firstname,o.employeeid,sum(totalamount) as sales_amount from new_orders o
left join new_employees e on e.employeeid = o.employeeid
group by o.employeeid)
select employeeid,firstname,sales_amount
from cte
order by sales_amount desc
limit 3;



select employeeid,sum(totalamount) as sales_amount,extract(month from orderdate) as  month
from new_orders
group by employeeid,month
having month = 1;

SELECT EmployeeID, SUM(TotalAmount) AS TotalSalesAmount
FROM new_Orders
WHERE YEAR(OrderDate) = 2022 AND MONTH(OrderDate) = 1
GROUP BY EmployeeID;

Select * from new_orders;

Select * from new_products;

Select productid,productname,price
from new_products
order by price desc
limit 1;

select n.category,avg(o.quantity) as avg_qty from new_products n
left join new_orders o on o.productid = n.productid
group by n.category
order by avg_qty;

SELECT p.Category, AVG(o.Quantity) AS AvgOrderQuantity
FROM new_Orders o
JOIN new_Products p ON o.ProductID = p.ProductID
GROUP BY p.Category;


Select orderid,orderdate,
lag (orderdate) over(order by orderdate) as next_order
from 
new_orders;

SELECT 
    salesDate,
    SalesAmount,
    LAG(SalesAmount) OVER (ORDER BY salesDate) AS PrevMonthSalesAmount,
    ((SalesAmount - LAG(SalesAmount) OVER (ORDER BY salesDate)) / LAG(SalesAmount) OVER (ORDER BY salesDate)) AS MoMGrowth
FROM 
    Sales;
    
    
    
    SELECT 
    EXTRACT(YEAR FROM salesDate) AS Year,
    EXTRACT(MONTH FROM salesDate) AS Month,
    SalesAmount,
    LAG(SalesAmount, 12) OVER (ORDER BY salesDate) AS PrevYearSalesAmount,
    ((SalesAmount - LAG(SalesAmount, 12) OVER (ORDER BY salesDate)) / LAG(SalesAmount, 12) OVER (ORDER BY salesDate)) AS YoYGrowth
FROM 
    Sales;

