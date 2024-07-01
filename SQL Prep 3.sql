Select * from employees;

select s.name as manager_name,s.managerid,e.name employee_name
from employees s 
left join employees e on e.managerID = s.EmployeeID;


SELECT e.Name AS EmployeeName, 
       e.Department AS EmployeeDepartment, 
       m.Name AS ManagerName, 
       m.Department AS ManagerDepartment
FROM Employees e
LEFT JOIN Employees m ON e.ManagerID = m.EmployeeID;



CREATE TABLE Orders (
    OrderID INT PRIMARY KEY,
    CustomerID INT,
    OrderDate DATE,
    TotalAmount DECIMAL(10, 2)
);

INSERT INTO Orders (OrderID, CustomerID, OrderDate, TotalAmount)
VALUES
(1, 101, '2024-01-05', 100.00),
(2, 102, '2024-01-10', 150.00),
(3, 103, '2024-02-15', 200.00),
(4, 101, '2024-03-20', 120.00),
(5, 104, '2024-03-25', 180.00),
(6, 105, '2024-04-02', 250.00),
(7, 102, '2024-04-10', 300.00),
(8, 106, '2024-05-15', 180.00),
(9, 101, '2024-06-20', 220.00),
(10, 104, '2024-06-25', 170.00);


select customerid,sum(totalamount) over (order by orderdate) as cumulativesum
from orders;

SELECT OrderID, OrderDate, TotalAmount,
       SUM(TotalAmount) OVER (ORDER BY OrderDate) AS CumulativeSum
FROM Orders;

SELECT OrderID, OrderDate, TotalAmount,
       RANK() OVER (PARTITION BY EXTRACT(Year_MONTH FROM OrderDate) ORDER BY TotalAmount DESC) AS Rankn
FROM Orders;


SELECT OrderID, OrderDate, TotalAmount,
       TotalAmount - LAG(TotalAmount) OVER (ORDER BY OrderDate) AS AmountDifference
FROM Orders;


SELECT 
    OrderID,
    OrderDate,
    TotalAmount,
    LEAD(OrderDate) OVER (ORDER BY OrderDate) AS NextOrderDate,
    LEAD(TotalAmount) OVER (ORDER BY OrderDate) AS NextTotalAmount
FROM Orders;