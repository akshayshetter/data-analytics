Select * from family;

with adult as
(select *, row_number() over (order by person desc) as rn
from family
where type = 'Adult'),

child as (select * ,row_number() over (order by person) as rn
from family
where type = 'child')
select a.person,c.person,a.age,c.age
from adult a left join 
child c on a.rn = c.rn;

