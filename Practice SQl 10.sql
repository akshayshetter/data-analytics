Select * from icc_world_cup;

with all_matches as
(select team, sum(no_of_matches_played) as matches_played from
(Select team_1 as team,count(*) as no_of_matches_played from icc_world_cup group by team_1 
union all
select team_2 as team, count(*) as no_of_matches_played from icc_world_cup group by team_2) A
group by team)

Select winner,count(*) as wins from icc_world_cup group by winner;