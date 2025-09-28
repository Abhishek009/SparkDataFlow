create schema sdf_schema;

CREATE TABLE sdf_schema.sdf_framework_source_one (
  id int DEFAULT NULL,
  first_name text,
  last_name text,
  accountnumber text,
  cardnumber bigint DEFAULT NULL
);

CREATE TABLE sdf_schema.sdf_framework_source_two (
  id int DEFAULT NULL,
  first_name text,
  last_name text,
  accountnumber text,
  cardnumber bigint DEFAULT NULL
);

CREATE TABLE sdf_schema.sdf_framework_source_two (
  id int DEFAULT NULL,
  first_name text,
  last_name text,
  accountnumber text,
  cardnumber bigint DEFAULT NULL
);

CREATE TABLE sdf_schema.spark_sdf_join(
id text,
x_first_name text,
x_last_name text,
x_cardnumber text,
x_accountnumber text,
y_first_name text,
y_last_name text,
y_cardnumber text,
y_accountnumber text
);

-- for sql server
drop table sdf_schema.spark_sdf_join;
CREATE TABLE sdf_schema.spark_sdf_join(
id int,
x_first_name nvarchar(50),
x_last_name nvarchar(50),
x_cardnumber bigint,
x_accountnumber nvarchar(50),
y_first_name nvarchar(50),
y_last_name nvarchar(50),
y_cardnumber bigint,
y_accountnumber nvarchar(50)
);

Select * from sdf_schema.spark_sdf_join;

select
x.id as id,
x.first_name as x_first_name,
x.last_name as x_last_name,
x.cardnumber as x_cardnumber,
x.accountnumber as x_accountnumber,
y.first_name as y_first_name,
y.last_name as y_last_name,
y.cardnumber as y_cardnumber,
y.accountnumber as y_accountnumber
from sdf_framework_source_one x
left join sdf_framework_source_two y
on x.id=y.id;

Select * from sdf_schema.spark_sdf_join;

create table sparkdataflow.t1 as Select m.batsman,b.city
from mysqlread m
join books b
on
m.match_id=b.id
where b.city="Hyderabad";

select count(*) from sparkdataflow.t1;