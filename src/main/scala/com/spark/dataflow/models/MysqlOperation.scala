package com.spark.dataflow.models

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object MysqlOperation {

  /*
  @param spark:SparkSession
  @param connectionUrl:String
  @param databaseName:String
  @param tableName:String
  @param userName:String
  @param password:String
   */
  def getJdbcDF(spark:SparkSession,
                     connectionUrl:String,
                     databaseName:String,
                     tableName:String,
                     userName:String,password:String): DataFrame = {

    val jdbcDf = spark.read.format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", s"${connectionUrl}/${databaseName}")
      .option("dbtable", tableName)
      .option("user", userName)
      .option("password", password)
      .load()

    jdbcDf.show()
    jdbcDf
  }

  def writeToJdbc(spark: SparkSession,
                  tempView:String, df_name:String,
                  databaseName:String,tableName:String,outputType:String,
                  identifier:String,connectionUrl:String,
                  userName:String,password:String,
                 mode:String):Unit={

    val df = spark.table(s"${tempView}")
    try {
      df.write
        .format(s"${outputType}")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("url", s"${connectionUrl}/${databaseName}")
        .option("dbtable", tableName)
        .option("user", userName)
        .option("password", password)
        .mode(mode).save()
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }

  }

}
