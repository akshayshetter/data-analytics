select * from namaste_orders n
left join namaste_returns r on r.order_id = n.order_id;


select n.city, count(r.order_id) as no_return from namaste_orders n
left join namaste_returns r on r.order_id = n.order_id
group by n.city
having count(r.order_id) = 0;