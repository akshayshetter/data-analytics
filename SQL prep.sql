select * from customers;
select * from orders;
select * from orderitems;
select * from products;

select c.*,o.* 
from customers c
join orders o on o.customerid = c.customerid
where orderdate = '2023-05-12';


with cte as(select c.customerid, o.orderdate
from customers c
join orders o on o.customerid = c.customerid)
select customerid, count(orderdate) as no_of_orders
from cte
group by customerid;


SELECT CustomerID, COUNT(*) AS TotalOrders
FROM Orders
GROUP BY CustomerID;


Select p.productname,p.productid as product, sum(oi.quantity) as total_quantity
from products p
join orderitems oi on oi.productid = p.productid
group by p.productid
order by total_quantity desc
limit 5;


SELECT p.ProductName, SUM(oi.Quantity) AS TotalQuantitySold
FROM OrderItems oi
JOIN Products p ON oi.ProductID = p.ProductID
GROUP BY oi.ProductID
ORDER BY TotalQuantitySold DESC
LIMIT 5;

Select a.category,sum(a.total_qty * a.price) as total_revenue
from
(select p.category, sum(oi.quantity) as total_qty,p.price
from 
orderitems oi 
left join products p on p.productid = oi.productid
group by p.category,p.price) a 
group by a.category;

SELECT p.Category, SUM(oi.Quantity * oi.Price) AS TotalRevenue
FROM OrderItems oi
JOIN Products p ON oi.ProductID = p.ProductID
GROUP BY p.Category;

USE BLACKBUCK;
SELECT Category, OrderDate, SUM(TotalAmount) OVER (PARTITION BY Category ORDER BY OrderDate) AS RunningTotalSales
FROM Orders o
JOIN OrderItems oi ON o.OrderID = oi.OrderID
JOIN Products p ON oi.ProductID = p.ProductID
ORDER BY Category, OrderDate;

SELECT Category, OrderDate, SUM(TotalAmount) OVER (PARTITION BY Category ORDER BY OrderDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS RollingTotalSales
FROM Orders o
JOIN OrderItems oi ON o.OrderID = oi.OrderID
JOIN Products p ON oi.ProductID = p.ProductID
ORDER BY Category, OrderDate;

SELECT Category,
       SUM(CASE WHEN EXTRACT(YEAR FROM OrderDate) = 2023 THEN TotalAmount ELSE 0 END) AS Year_2023,
       SUM(CASE WHEN EXTRACT(YEAR FROM OrderDate) = 2024 THEN TotalAmount ELSE 0 END) AS Year_2024
FROM Orders o
JOIN OrderItems oi ON o.OrderID = oi.OrderID
JOIN Products p ON oi.ProductID = p.ProductID
GROUP BY Category;

SELECT COUNT(*)
FROM CUSTOMERS C
JOIN ORDERS O ON O.CUSTOMERID = C.CUSTOMERID;