create table ipl.deliveries(
match_id varchar(50),
inning varchar(50),
batting_team varchar(50),
bowling_team varchar(50),
over varchar(50),
ball varchar(50),
batsman varchar(50),
non_striker varchar(50),
bowler varchar(50),
is_super_over varchar(50),
wide_runs varchar(50),
bye_runs varchar(50),
legbye_runs varchar(50),
noball_runs varchar(50),
penalty_runs varchar(50),
batsman_runs varchar(50),
extra_runs varchar(50),
total_runs varchar(50),
player_dismissed varchar(50),
dismissal_kind varchar(50),
fielder varchar(50)
);

create table sparkdataflow.t1 as Select m.batsman,b.city
from mysqlread m
join books b
on
m.match_id=b.id
where b.city="Hyderabad";
select count(*) from sparkdataflow.t1;