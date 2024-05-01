package com.spark.dataflow.models

import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveOperation {

  def getHiveDF(spark: SparkSession, tableName: String, schemaName: String): DataFrame ={
    val hiveDF = spark.read.table(s"${schemaName}.${tableName}")
    hiveDF
  }

def writeToHiveTable(spark: SparkSession,tempView:String,outputTableName:String,
                     outputSchemaName:String,partition:String,mode:String): Unit = {
  val df = spark.table(s"${tempView}")
  try {
    if(partition.isEmpty) {
      df.write.mode(mode).save()
    }else{
      df.write.partitionBy(partition).insertInto(s"${outputSchemaName}.${outputTableName}")
    }
  }
  catch {
    case e: Exception => {
      e.printStackTrace()
    }
  }
}

}
