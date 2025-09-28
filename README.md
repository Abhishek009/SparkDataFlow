# SparkDataFlow
SDF(Spark Data Flow) is an ETL tool which allows users to quickly write etl jobs based on the sql queries.

**job_FileToMysql.yml**

```jobName: FileToMySql

jobName: FileToMysql
engine: spark
job:
  - input:
      df-name: mysql_customer_info
      type: file
      identifier: local
      path: D:/SampleData/mock_data/sdf_sample_data.csv
      option: |
        delimiter=,
        header=true

  - transform:
      df-name: t1
      t_inputs: mysql_customer_info
      query: "Select * from mysql_customer_info limit 10"
      output: out-01

  - output:
      df-name: out-01
      type: jdbc
      identifier: mysql
      table: sdf_framework_source_one
      schema: sdf_schema 
```


**config.yml**

```
jdbc:
    mysql:
      username: root
      password: root
      url: jdbc:mysql://localhost:3306
      driver: com.mysql.cj.jdbc.Driver
    postgres:
      username: postgres
      password: root
      url: jdbc:postgresql://localhost:5432
      driver: org.postgresql.Driver
    sqlserver:
      username: postgres
      password: root
      url: jdbc:postgresql://localhost:5432
      driver: org.postgresql.Driver
file:
  - local:
      location: D:\SampleData\IPL\matches.csv
  
hive:

```