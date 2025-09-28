# SparkDataFlow
SDF(Spark Data Flow) is an ETL tool which allows users to quickly write etl jobs based on the sql queries.

--configFile D:\Google_Drive_Rahul\GitHub\SparkDataFlow\src\main\resources\config.yml
--jobFile D:\Google_Drive_Rahul\GitHub\SparkDataFlow\example-yml\job_fileToMysql.yml

**job_HiveToFile.yml**

```jobName: HiveToFile

job:
  - input:
      df-name: etl_framework_source_one
      type: hive
      identifier: hive
      table: etl_framework_source_one
      schema: YOUR_SCHEMA_NAME

  - transform:
      df-name: t1
      t_inputs: etl_framework_source_one
      query: Select * from etl_framework_source_one
      output: out-01

  - output:
      df-name: out-01
      type: file
      identifier: hdfs
      path: /your/hdfs/location
      output_format: csv
      
```

**job_HiveToFile.yml**
```
jobName: mysqltolocal

job:
  - input:
      df-name: mysqlread
      type: jdbc
      identifier: mysql
      table: deliveries
      schema: ipl
  - input:
      df-name: books
      type: file
      identifier: local
      option: |
        delimiter=,
        header=true
      path: D:/SampleData/IPL/matches.csv

  - transform:
      df-name: t1
      t_inputs: mysqlread
      query: Select m.batsman,b.city from mysqlread m join books b on m.match_id=b.id where b.city="Hyderabad"
      output: out-01

  - transform:
      df-name: t2
      t_inputs: t1
      query: select count(*) from t1;
      output: out-02

  - output:
      df-name: out-01
      type: file
      identifier: local
      path: D:/SampleData/IPLSample
      output_format: csv


  - output:
      df-name: out-02
      type: jdbc
      identifier: mysql
      table: sampleipldata2
      schema: sparkdataflow
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

databricks:
  catalog: [optional catalog name if you are using Unity Catalog]
  schema: [schema name] # Required
  host: [yourorg.databrickshost.com] # Required
  http_path: [/sql/your/http/path] # Required
  token: [dapiXXXXXXXXXXXXXXXXXXXXXXX] # Required Personal Access Token (PAT) if using token-based authentication
```