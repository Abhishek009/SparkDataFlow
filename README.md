# SparkDataFlow
SDF(Spark Data Flow) is an ETL tool which allows users to quickly write etl jobs based on the sql queries.

**job.yml**
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
    postgress:
      username: root
      password: root
      url: jdbc://localhost/3306

file:
  - local:
      location: D:\SampleData\IPL\matches.csv
```