with cte2 as(with cte as(select m.genre,m.title,m.id,round(avg(rating),0) as avg_rate from movies m
join reviews r on r.movie_id = m.id
group by m.genre,m.title,m.id)
select genre,title,avg_rate,
row_number() over(partition by genre order by avg_rate desc) as rnk
from cte
)
select genre,title,avg_rate,rnk,Replicate('*',round(avg(rating),0)) as stars
from cte2
where rnk = 1
