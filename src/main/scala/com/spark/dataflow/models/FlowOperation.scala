package com.spark.dataflow.models

import com.spark.dataflow.configparser.{Input, Output, Transform}
import com.spark.dataflow.models.FileOperation.{logger, writeToFile}
import com.spark.dataflow.models.HiveOperation.writeToHiveTable
import com.spark.dataflow.models.MysqlOperation.writeToJdbc
import com.spark.dataflow.utils.CommonFunctions.writeToStaging
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.spark.dataflow.utils.{CommonConfigParser, SparkJob}
import org.apache.logging.log4j.{LogManager, Logger}

object FlowOperation {

  private var inputDataFrame:DataFrame = _
  var transformToOutputMapping:scala.collection.mutable.Map[String,String] =
    scala.collection.mutable.Map.empty[String, String]

  val logger: Logger = LogManager.getLogger(getClass.getSimpleName)

  def createInput(input: Input, spark: SparkSession, jobConfigFileName: String): DataFrame = {

    input.`type` match {
      case "jdbc" => {
        logger.info(s"Input df-name : ${input.`df-name`}")
        logger.info(s"Data Read will happen from : ${input.`type`}")
        logger.info(s"Identifier is : ${input.identifier}")
        logger.info(s"Database Name : ${input.schema.getOrElse("")}")

        val dfName= input.`df-name`
        val commonConfig = CommonConfigParser.parseConfig(jobConfigFileName, "jdbc", input.identifier)
        val connectionUrl = commonConfig.get("url")
        val userName = commonConfig.get("username")
        val password = commonConfig.get("password")
        val driver = commonConfig.get("driver")
        val databaseName = input.schema.getOrElse("")
        val tableName = input.table.getOrElse("")


        logger.info(s"Input table $tableName")
        logger.info(s"Input schema $databaseName")
        logger.info(s"Connection Url $connectionUrl")

        inputDataFrame = MysqlOperation.getJdbcDF(spark, connectionUrl,
          databaseName, tableName, userName, password,driver,input.identifier)
        SparkJob.createTempTable(spark,inputDataFrame,dfName)
      }
      case "file" => {
        logger.info(s"Input df-name : ${input.`df-name`}")
        logger.info(s"Input type : ${input.`type`}")
        val path = input.path.getOrElse("")
        logger.info(s"Data Read will happen from : $path")
        val identifier = input.identifier
        val option = input.option.getOrElse("")
        logger.info(s"Input other option : $option")
        inputDataFrame = FileOperation.getFileDF(spark, "", path, option)
        SparkJob.createTempTable(spark,inputDataFrame,input.`df-name`)

      }
      case "hive" => {
        logger.info(s"Input df-name ${input.`df-name`}")
        logger.info(s"Input type ${input.`type`}")
        logger.info(s"Data Read will happen from  ${input.`type`}")
        logger.info(s"Input Schema  ${input.schema}")
        val schemaName = input.schema.getOrElse("")
        logger.info(s"Input Table  ${input.table}")
        val tableName = input.table.getOrElse("")
        val identifier = input.identifier
        val option = input.option.getOrElse("")
        logger.info(s"Input other option $option")
        inputDataFrame = HiveOperation.getHiveDF(spark, tableName,schemaName)
        SparkJob.createTempTable(spark,inputDataFrame,input.`df-name`)
      }
    }
    inputDataFrame
  }

  def createTransformation(transform: Transform, spark: SparkSession):  scala.collection.mutable.Map[String, String] = {
      val df_transform = spark.sql(s"${transform.query}")
      logger.info(s"Creating temp view ${transform.`df-name`}")
      df_transform.createOrReplaceTempView(s"${transform.`df-name`}")
      logger.info(s"Transform df-name ${transform.`df-name`}")
      logger.info(s"Transform t_inputs ${transform.t_inputs.getOrElse("")}")
      logger.info(s"Transform query ${transform.query}")
      logger.info(s"Checking if sql model is a inline query or a file")
      logger.info(s"Transform output ${transform.output}")

      transformToOutputMapping += s"${transform.`df-name`}" -> s"${transform.output}"
      transformToOutputMapping
  }

  def createOutput(output: Output, spark: SparkSession,
                   transformToOutputMapping: scala.collection.mutable.Map[String, String],
                   jobConfigFileName: String): Unit = {

    output.`type` match {
      // Check if output.type is jdbc
      case "jdbc" => {
        transformToOutputMapping.foreach(f => {
          println(f._1, f._2)
          if (f._2 == output.`df-name`) {
            val commonConfig = CommonConfigParser.parseConfig(
              jobConfigFileName, "jdbc", output.identifier)

            logger.info(s"JDBC output.`df-name ${output.`df-name`}")
            logger.info(s"JDBC output.query ${output.`type`}")
            logger.info(s"JDBC output.table ${output.table}")
            logger.info(s"JDBC output.schema ${output.schema}")
            logger.info(s"JDBC output.identifier ${output.identifier}")
            logger.info(s"JDBC output.mode ${output.mode}")

            val connectionUrl = commonConfig.get("url")
            val userName = commonConfig.get("username")
            val password = commonConfig.get("password")
            val driver = commonConfig.get("driver")
            println("Connection Url=>"+connectionUrl)
            val tempView=f._1
            writeToJdbc(spark,tempView,output.`df-name`,
              output.schema.getOrElse(""),output.table.getOrElse(""),
              output.`type`,output.identifier,
              connectionUrl,userName,password,
              output.mode.getOrElse(""),driver)
          }
        }
        )
      }

      // Check if output type is file
      case "file" => {
        transformToOutputMapping.foreach(f => {
          println(f._1, f._2)
          if (f._2 == output.`df-name`) {
            logger.info(s"File output.df-name ${output.`df-name`}")
            logger.info(s"File output.t_inputs ${output.path}")
            logger.info(s"File output.type ${output.`type`}")
            logger.info(s"File output.identifier ${output.identifier}")
            logger.info(s"File output.output_format ${output.output_format}")
            logger.info(s"File output.option ${output.option}")
            logger.info(s"File output.mode ${output.mode}")
            val tempView=f._1
            writeToFile(spark,tempView,output.`df-name`,
              output.path.getOrElse(""),output.`type`,
              output.identifier,output.output_format.getOrElse(""),
              output.mode.getOrElse(""), output.option.getOrElse("")
              )
          }
        }
        )
      }
      // Check if output type is hive
      case "hive" => {
        transformToOutputMapping.foreach(f => {
          println(f._1, f._2)
          if (f._2 == output.`df-name`) {
            logger.info(s"Hive output.df-name ${output.`df-name`}")
            logger.info(s"Hive output.t_inputs ${output.path}")
            logger.info(s"Hive output.type ${output.`type`}")
            logger.info(s"Hive output.identifier ${output.identifier}")
            logger.info(s"Hive output.output_format ${output.output_format}")
            logger.info(s"Hive output.option ${output.option}")
            logger.info(s"Hive output.mode ${output.mode}")
            logger.info(s"Hive output.partition ${output.partition}")
            val tempView=f._1
            val outputTableName=output.table.getOrElse("")
            val outputSchemaName= output.schema.getOrElse("")
            val outputMode = output.mode.getOrElse("")
            val partition=output.partition.getOrElse("")
            writeToHiveTable(spark,tempView,
              outputTableName,outputSchemaName,
              partition,outputMode)

          }
        }
        )
      }

    }

  }


}
