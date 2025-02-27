use ig_clone;

#Oldest user
Select id,username,created_at as date_of_join
from users
order by date_of_join
Limit 5;

#User who have never Posted Photo

Select Username
from users
left join
Photos on users.id=photos.user_id
where Photos.id is null;

#User with max like
Select users.username,photos.id,photos.image_url,count(*) as total_likes
from likes
join photos on photos.id=photo_id
join users on users.id=likes.photo_id
group by photos.id
order by total_likes desc
limit 1;


# Most commonly used tags

Select tag_name,count(tag_name) as Total_tags
from tags
join photo_tags on tags.id=photo_tags.tag_id
group by tags.id
order by total_tags desc
limit 5;


# Most user registered week day

Select date_format(created_at,'%W') as week_day,count(*) as number_of_users
from users
group by 1;