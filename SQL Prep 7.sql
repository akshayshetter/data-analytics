Select * from orders;

select * from orderitems;
select * from products;
Select orderid,sum(quantity * unitprice) as total_amount
from orderitems
group by orderid;

Select o.productid,p.productname, sum(o.quantity) as units_sold
from orderitems o
left join products p on p.productid = o.productid
group by o.productid
order by units_sold desc
limit 1;

Select * from customers;
select * from orders;
select * from orderitems;

Select c.customerid,c.firstname, count(orderid) as total_num_orders from customers c
left join orders o on o.customerid = c.customerid
group by c.customerid;

Select * from categories;
select * from orderitems;
select * from orders;
select * from products;

Select c.categoryname,sum(o.quantity * o.unitprice) as total_sales from products p
left join categories c on c.categoryid = p.categoryid
left join orderitems o on o.productid = p.productid
group by c.categoryname
order by total_sales desc
limit 1;

SELECT cat.CategoryName, SUM(oi.Quantity * oi.UnitPrice) AS TotalSalesAmount
FROM Categories cat
JOIN Products p ON cat.CategoryID = p.CategoryID
JOIN OrderItems oi ON p.ProductID = oi.ProductID
GROUP BY cat.CategoryName
ORDER BY TotalSalesAmount DESC
LIMIT 1;

Select * from customers;

Select * from orders;

Select c.firstname,c.lastname from 
customers c
left join orders o on o.customerid = c.customerid
where o.customerid is null;


SELECT c.FirstName, c.LastName
FROM Customers c
LEFT JOIN Orders o ON c.CustomerID = o.CustomerID
WHERE o.CustomerID IS NULL;

Select * from orders;
Select * from customers;

Select o.orderid,c.customerid,c.firstname,o.orderdate, o.totalamount as total_amount, c.lastname,extract(month from orderdate) as order_month from orders o
left join customers c on c.customerid = o.customerid
group by c.customerid,o.orderid
having order_month = 1;

Select * from products;
Select * from orders;
select * from customers;
select * from orderitems;
select * from categories;

Select c.customerid,c.firstname,oi.orderitemid,oi.productid,oi.unitprice,oi.quantity,p.productname from customers c
left join orders o on o.customerid = c.customerid
left join orderitems oi on oi.orderid = o.orderid
left join products p on p.productid = oi.productid
where c.customerid = 1;

SELECT p.ProductName, oi.Quantity, oi.UnitPrice
FROM Products p
JOIN OrderItems oi ON p.ProductID = oi.ProductID
JOIN Orders o ON oi.OrderID = o.OrderID
WHERE o.CustomerID = 1;

Select c.customerid,c.firstname,avg(totalamount) as avg_order from customers c
left join orders o on o.customerid = c.customerid
group by c.customerid;

SELECT c.FirstName, c.LastName, AVG(o.TotalAmount) AS AverageOrderAmount
FROM Customers c
LEFT JOIN Orders o ON c.CustomerID = o.CustomerID
GROUP BY c.FirstName, c.LastName;
