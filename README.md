# SparkDataFlow

A modular ETL framework in Scala supporting YAML-based job configuration, Spark transformations, and flexible input/output sources.

## Features

- **YAML-driven ETL**: Define jobs, inputs, transforms, and outputs in YAML.
- **Custom Transform Scripts**: Supports SQL, Python, and Scala scripts for data transformation.
- **Multiple Engines**: Run jobs on Spark or Databricks.
- **Robust Error Handling**: Logging and graceful failure for YAML parsing, Spark jobs, and database operations.

## Getting Started

### Prerequisites

- Java 11+
- Scala 2.12+
- Apache Spark 3.x
- Maven

## Run
```shell
spark-submit --class com.spark.dataflow.Flow target/sparkdataflow-1.0.jar --jobFile <job_file.yml> --jobConfig <config.yml> --configFile <meta_config.json>
```
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