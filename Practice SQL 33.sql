Create database lilly;

Use lilly;

Select d.doctorname,d.doctorid from customerinteractions c
left join doctors d on c.doctorid = d.doctorid
where c.interactiontype ='Email'
group by d.doctorname,d.doctorid;

SELECT d.*
FROM Doctors d
JOIN CustomerInteractions ci ON d.DoctorID = ci.DoctorID
WHERE ci.InteractionType = 'Email';


Select d.doctorname,d.doctorid,s.salesamount, sum(salesamount)as total_amount from salesdata s
join doctors d on s.doctorid = d.doctorid
group by d.doctorname,d.doctorid,s.salesamount
order by s.salesamount desc;

Select d.doctorname, c.doctorid,c.interactiondate from customerinteractions c
join doctors d on d.doctorid = c.doctorid
where InteractionDate >= '2024-04-03'
group by d.doctorname,c.DoctorID,c.interactiondate;

Select avg(SalesAmount) as avg_sales, extract(month from transactiondate) as month from salesdata
group by month;

select * from salesdata;

Select d.doctorname, d.doctorid from customerinteractions c
join doctors d on c.doctorid = d.doctorid
join salesdata s on s.doctorid = d.doctorid
where c.interactionid and s.TransactionID is not null
group by d.DoctorName,d.DoctorID;

SELECT DISTINCT d.*
FROM Doctors d
JOIN SalesData sd ON d.DoctorID = sd.DoctorID
JOIN CustomerInteractions ci ON d.DoctorID = ci.DoctorID;


Select d.doctorname,d.doctorid, max(s.salesamount) as high_sales from salesdata s
join doctors d on d.doctorid = s.doctorid
group by d.doctorname, d.doctorid
order by high_sales desc
limit 1;

SELECT d.*
FROM Doctors d
JOIN SalesData sd ON d.DoctorID = sd.DoctorID
WHERE sd.SalesAmount = (
    SELECT MAX(SalesAmount)
    FROM SalesData
);

with cte as 
(Select p.productname,o.quantity,p.unitprice,p.productid,(p.unitprice * o.quantity) as total_revenue from products p
join orders o on o.productid = p.productid
group by p.productname,o.quantity,p.unitprice,p.productid,(p.unitprice * o.quantity)
)
Select productname,
sum(total_revenue) over(partition by productid order by total_revenue ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as product_revenue
from cte;

Select d.DoctorID,d.doctorname,Sum(o.quantity) as total_quantity from orders o
join products p on p.productid = o.productid
join doctors d on o.doctorid = d.doctorid
group by d.doctorid,d.doctorname
order by total_quantity desc;

Select productname,max(unitprice) as highest_price from products
group by productname
limit 1