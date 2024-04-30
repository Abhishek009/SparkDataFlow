package com.spark.dataflow.models

import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveOperation {

  def getHiveDF(spark: SparkSession, tableName: String, schemaName: String): DataFrame ={
    val hiveDF = spark.read.table(s"${schemaName}.${tableName}")
    hiveDF
  }

  def writeToHive()

}
