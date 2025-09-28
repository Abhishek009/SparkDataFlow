--Create table without partition
create external table sdf_schema.sdf_framework_source_one (
id string,
first_name string,
last_name string,
cardnumber string,
accountnumber string
)
ROW FORMAT DELIMITER
FIELDS TERMINATED BY ','
LOCATION 'YOUR HDFS lOCATION'

--Create table without partition
create external table sdf_schema.sdf_framework_source_two (
id string,
first_name string,
last_name string,
cardnumber string,
accountnumber string
)
ROW FORMAT DELIMITER
FIELDS TERMINATED BY ','
LOCATION 'YOUR HDFS lOCATION'

--Create table with partition
create external table sdf_schema.sdf_framework_source_partition_one (
first_name string,
last_name string,
cardnumber string,
accountnumber string
)
PARTITIONED BY (id string)
ROW FORMAT DELIMITER
FIELDS TERMINATED BY ','
LOCATION 'YOUR HDFS lOCATION'

--Create table with partition
create external table sdf_schema.spark_sdf_join (
x_first_name String,
x_last_name String,
x_cardnumber String,
x_accountnumber String,
y_first_name String,
y_last_name String,
y_cardnumber String,
y_accountnumber String
)
PARTITIONED BY (id string)
ROW FORMAT DELIMITER
FIELDS TERMINATED BY ','
LOCATION 'YOUR HDFS lOCATION'


--Select statement with left join
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
on x.id=y.id