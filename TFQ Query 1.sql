select * from brands;

select b.brand1, b.brand2, b.custom1, b.custom2, b.custom3, b.custom4, b.year
from
(select *,
 row_number() over(partition by a.pair_id order by a.pair_id) as rn
from
(select *, case when brand1 < brand2 then concat(brand1,brand2,year)
     else concat(brand2,brand1,year) end as pair_id
     from  brands) a
)b
 where b.rn=1
 or b.custom1 != b.custom3 and b.custom2 != b.custom4
 order by b.year;