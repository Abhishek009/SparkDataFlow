package com.spark.dataflow.utils


import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkJob {

  val logger: Logger = LogManager.getLogger(this.getClass.getName)
  var spark:SparkSession = _

  /**
   * @param appName
   * @param master
   */
  def createSparkSession(appName: String, master: String): SparkSession = {

    logger.info(s"Master: ${master}")
    logger.info(s"AppName: ${appName}")
    master match {
      case "local" => spark = SparkSession.builder().appName(appName).master("local[1]").getOrCreate()
      case _ => {
        spark = SparkSession.builder().appName(appName).enableHiveSupport().getOrCreate()
      }
    }
    logger.info(s"spark: ${spark}")
    spark
  }

  /**
   * @param appName
   * @param master
   * @param session
   */
  def isSparkSessionAvailable(appName: String, master: String, session: SparkSession): Unit = {
    val sparkSession = session match {
      case ss => ss
      case _ => createSparkSession(appName, master)
    }
  }

  def createTempTable(spark: SparkSession, df: DataFrame,viewName:String): Unit = {
    df.createOrReplaceTempView(viewName)
  }



}
