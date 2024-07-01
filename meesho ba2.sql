Select * from customer;
select * from product;
select * from purchase;

with cte as(Select category,product_id,discount,
row_number() over (partition by category order by discount desc) as rnk
from product)
select category, product_id,discount
from cte 
where rnk = 1;


WITH MaxDiscount AS (
    SELECT 
        p.CATEGORY,
        MAX(p.DISCOUNT) AS max_discount
    FROM 
        PRODUCT p
    GROUP BY 
        p.CATEGORY
)

SELECT 
    p.CATEGORY,
    MIN(p.PRODUCT_ID) AS PRODUCT_ID,
    p.DISCOUNT
FROM 
    PRODUCT p
    INNER JOIN MaxDiscount md ON p.CATEGORY = md.CATEGORY AND p.DISCOUNT = md.max_discount
GROUP BY 
    p.CATEGORY, p.DISCOUNT
ORDER BY 
    p.CATEGORY ASC;