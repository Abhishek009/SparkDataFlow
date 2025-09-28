create table sparkdataflow.t1 as Select m.batsman,b.city
from mysqlread m
join books b
on
m.match_id=b.id
where b.city="Hyderabad";
select count(*) from sparkdataflow.t1;