SELECT coalesce(s.id,t.id) as id,-- s.name,t.name,
case when t.name is null then 'new in source' else 'mismatch' end as comment
FROM source s 
LEFT JOIN target t ON s.id = t.id
WHERE t.name != s.name OR t.name IS NULL OR s.name IS NULL
UNION
SELECT coalesce(t.id,s.id) as id,-- s.name,t.name,
case when s.name is null then 'new in target' else 'mismatch' end as comment
FROM source s
RIGHT JOIN target t ON s.id = t.id
WHERE t.name != s.name OR t.name IS NULL OR s.name IS NULL;


Select f.cid,f.origin,g.destination from flights f
inner join flights g on f.destination = g.origin;


use youtubeinterview;
select * from sales;

select count(customer) as total_count,Extract(month from order_date) as months from
(select * ,row_number() over (partition by customer order by order_date asc) as rnk
from sales) a
where rnk = 1
group by months;

