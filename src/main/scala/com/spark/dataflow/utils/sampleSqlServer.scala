package com.spark.dataflow.utils


import org.apache.spark.sql.SparkSession

object sampleSqlServer {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("jhgkjh").master("local[1]").getOrCreate()
    val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

    val database_host = "my-first-azure-sql-server.database.windows.net"
    val database_port = "1433"
    val database_name = "my-first-azure-db"
    val table = "sdf_schema.sdf_framework_source_one"
    val user = "SqlDbAdmin@my-first-azure-sql-server"
    val password = "Alibaba@02"

    val url = s"jdbc:sqlserver://${database_host}:${database_port};database=${database_name}" +
      s";encrypt=true;trustServerCertificate=true;"
   //jdbc:sqlserver://my-first-azure-sql-server.database.windows.net:1433;database=my-first-azure-db;encrypt=true;trustServerCertificate=true;
   //jdbc:sqlserver://my-first-azure-sql-server.database.windows.net:1433;database=my-first-azure-db;encrypt=true;trustServerCertificate=true;;database=sdf_schema

    print(url)
    val remote_table = spark.read
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("dbtable", table)
      .option("user", user)
      .option("password", password)
      .load()

    /*val jdbcDf = spark.read.format("jdbc")
      .option("driver", driver)
      .option("url", s"${connectionUrl}/${databaseName}")
      .option("dbtable", tableName)
      .option("user", userName)
      .option("password", password)
      .load()*/


  }
}
