Use new_data;

Select * from sales;
Select * from customers;

Select s.saleid,c.customername,c.city from customers c
left join sales s on c.customerid = s.customerid
where c.city ='New York';

Select C.Customerid from customers c
join sales s on s.customerid = c.customerid
where s.saleid is null;

Select * from customers
where customerid not in
(Select distinct customerid from sales);


Select c.customername,s.saleid from sales s
left join customers c on c.customerid = s.customerid
where c.customername like 'a%';

SELECT *
FROM Sales
WHERE EXISTS (
    SELECT *
    FROM Customers
    WHERE Customers.CustomerID = Sales.CustomerID
      AND CustomerName LIKE 'A%'
);
