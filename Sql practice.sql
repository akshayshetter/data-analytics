# Query for 5 oldest users

Select id,username,created_at from users
where id or username or created_at is not null
order by created_at
limit 5;

#Users who never posted photo

Select distinct username
from users
left join photos
on users.username=photos.user_id
where image_url is null;


