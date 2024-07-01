Select * from products;
Select * from orders;
Select * from orderitems;

Select p.categoryid,sum(oi.quantity * oi.UnitPrice) as sales_amount from products p
left join orderitems oi  on oi.productid = p.productid
group by p.categoryid
order by sales_amount desc;

SELECT p.CategoryID, SUM(oi.Quantity * oi.UnitPrice) AS TotalSalesAmount
FROM OrderItems oi
JOIN Products p ON oi.ProductID = p.ProductID
GROUP BY p.CategoryID;

Select * from customers;
Select * from orders;

Select c.firstname,c.lastname,c.customerid,o.totalamount as amount_spent from customers c 
join orders o on o.customerid = c.customerid
group by c.customerid,amount_spent
order by amount_spent desc
limit 5;

SELECT c.CustomerID, c.FirstName, c.LastName, SUM(o.TotalAmount) AS TotalSpent
FROM Customers c
JOIN Orders o ON c.CustomerID = o.CustomerID
GROUP BY c.CustomerID, c.FirstName, c.LastName
ORDER BY TotalSpent DESC
LIMIT 5;

Select avg(totalamount) as avg_amount, extract(month from orderdate) as order_month 
from orders
group by  order_month
order by order_month desc;

SELECT EXTRACT(MONTH FROM o.OrderDate) AS Month, AVG(o.TotalAmount) AS AverageOrderAmount
FROM Orders o
WHERE EXTRACT(YEAR FROM o.OrderDate) = 2022
GROUP BY EXTRACT(MONTH FROM o.OrderDate);

Select * from products;

Select * from orderitems;

Select p.productid , count(oi.OrderItemID) as order_count from products p
join orderitems oi on oi.productid = p.ProductID
group by p.ProductID
having count(oi.orderitemid) > 5;

SELECT p.ProductName, COUNT(oi.OrderItemID) AS OrderCount
FROM Products p
JOIN OrderItems oi ON p.ProductID = oi.ProductID
GROUP BY p.ProductName
HAVING COUNT(oi.OrderItemID) > 5;

Select * FROM ORDERS;
Select * from orderitems;
Select * from customers;

Select o.customerid,count(o.orderid) as no_of_orders,extract(month from o.orderdate) as order_month from orders o 
left join customers c  on c.customerid = o.customerid
group by o.customerid,order_month
having order_month = 1;

SELECT c.CustomerID, COUNT(o.OrderID) AS TotalOrders
FROM Customers c
LEFT JOIN Orders o ON c.CustomerID = o.CustomerID
WHERE EXTRACT(YEAR FROM o.OrderDate) = 2022 AND EXTRACT(MONTH FROM o.OrderDate) = 1
GROUP BY c.CustomerID;

Select * from orders;
select * from products;
Select * from orderitems;


Select a.productid,a.rnk,a.total_quantity,a.categoryid
from 
(Select p.categoryid,p.productid,sum(oi.quantity) as total_quantity,
rank() over(partition by p.categoryid order by sum(oi.quantity)) as rnk
from orderitems oi
join products p on p.productid = oi.productid
group by p.categoryid,p.productid) a
group by a.categoryid,a.productid,a.total_quantity
having  rnk = 1;

Select c.firstname,c.lastname from orders o
left join customers c on c.customerid = o.customerid
where o.orderid is null;

SELECT c.CustomerID, c.FirstName, c.LastName
FROM Customers c
LEFT JOIN Orders o ON c.CustomerID = o.CustomerID
WHERE o.OrderID IS NULL;



Select * from orders;

select * from orderitems;

with cte as (Select o.customerid, sum(totalamount) as amount_spent, extract(month from o.orderdate) as order_month,
extract(year from orderdate) as order_year, rank() over (partition by o.customerid order by extract(month from o.orderdate)) as rnk
from orders o
group by o.customerid, order_month,order_year)

select customerid, order_month, amount_spent from cte
where rnk <= 3;

WITH RankedCustomers AS (
    SELECT c.CustomerID, 
           EXTRACT(MONTH FROM o.OrderDate) AS Month,
           SUM(o.TotalAmount) AS TotalSpent,
           ROW_NUMBER() OVER (PARTITION BY EXTRACT(MONTH FROM o.OrderDate) ORDER BY SUM(o.TotalAmount) DESC) AS rnk
    FROM Customers c
    JOIN Orders o ON c.CustomerID = o.CustomerID
    WHERE EXTRACT(YEAR FROM o.OrderDate) = 2022
    GROUP BY c.CustomerID, EXTRACT(MONTH FROM o.OrderDate)
)
SELECT CustomerID, Month, TotalSpent
FROM RankedCustomers
WHERE rnk <= 3;


Select * from products;

Select * from orderitems;
select * from orders;

Select p.categoryid,o.orderdate, 
sum(oi.Quantity * oi.UnitPrice) over(partition by p.CategoryID order by o.orderdate) as sales_amount
 from orderitems oi
left join products p on p.productid = oi.productid
left join orders o on o.orderid = oi.orderid;

SELECT p.CategoryID, 
       o.OrderDate,
       SUM(oi.Quantity * oi.UnitPrice) OVER (PARTITION BY p.CategoryID ORDER BY o.OrderDate) AS CumulativeSalesAmount
FROM Products p
JOIN OrderItems oi ON p.ProductID = oi.ProductID
JOIN Orders o ON oi.OrderID = o.OrderID;


Select * from products;
select * from orderitems;

Select a.categoryid,a.productid,avg_ordersqty
from
(Select p.productid, p.categoryid,avg(oi.quantity) as avg_ordersqty,
row_number() over (partition by p.categoryid order by avg(oi.quantity)) as rnk
from orderitems oi
left join products p on p.productid = oi.productid
group by p.productid,p.categoryid) 
a
where a.rnk <=5
group by a.CategoryID, a.productid
order by avg_ordersqty desc
 ;


WITH RankedProducts AS (
    SELECT p.ProductID, p.ProductName, p.CategoryID,
           AVG(oi.Quantity) AS AvgOrderQuantity,
           ROW_NUMBER() OVER (PARTITION BY p.CategoryID ORDER BY AVG(oi.Quantity) DESC) AS rnk
    FROM Products p
    JOIN OrderItems oi ON p.ProductID = oi.ProductID
    GROUP BY p.ProductID, p.ProductName, p.CategoryID
)
SELECT rp.ProductID, rp.ProductName, c.CategoryName, rp.AvgOrderQuantity
FROM RankedProducts rp
JOIN Categories c ON rp.CategoryID = c.CategoryID
WHERE rp.rnk <= 5;


Select * from customers;
select * from orders;

Select C.customerid, o.orderid, extract(month from o.orderdate) as order_month
from customers c
left join orders o on o.customerid = c.customerid
group by c.customerid, o.orderid
having order_month = 2 and order_month = 1;

WITH MonthlyOrders AS (
    SELECT DISTINCT CustomerID,
           EXTRACT(YEAR FROM OrderDate) AS Year,
           EXTRACT(MONTH FROM OrderDate) AS Month
    FROM Orders
)
SELECT mo1.CustomerID
FROM MonthlyOrders mo1
JOIN MonthlyOrders mo2 ON mo1.CustomerID = mo2.CustomerID
WHERE mo1.Year = 2022 AND mo1.Month = 1
  AND mo2.Year = 2022 AND mo2.Month = 2;


