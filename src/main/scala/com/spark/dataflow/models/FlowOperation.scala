package com.spark.dataflow.models

import com.spark.dataflow.configparser.{Input, Output, Transform}
import com.spark.dataflow.models.FileOperation.writeToFile
import com.spark.dataflow.models.MysqlOperation.writeToJdbc
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
        logger.info(s"input.df-name ${input.`df-name`}")
        logger.info(s"Data Read will happen from ${input.`type`}")


        val dfName= input.`df-name`
        val commonConfig = CommonConfigParser.parseConfig(jobConfigFileName, "jdbc", input.identifier)
        val connectionUrl = commonConfig.get("url")
        val userName = commonConfig.get("username")
        val password = commonConfig.get("password")
        val databaseName = input.schema.getOrElse("")
        val tableName = input.table.getOrElse("")
        logger.info(s"input.table $tableName")
        logger.info(s"input.schema $databaseName")
        logger.info(s"Connection Url $connectionUrl")

        inputDataFrame = MysqlOperation.getJdbcDF(spark, connectionUrl, databaseName, tableName, userName, password)
        SparkJob.createTempTable(spark,inputDataFrame,dfName)
      }
      case "file" => {
        logger.info(s"input.df-name ${input.`df-name`}")
        logger.info(s"input.type ${input.`type`}")
        logger.info(s"Data Read will happen from  ${input.`type`}")
        val path = input.path.getOrElse("")
        logger.info(s"input.path $path")
        val identifier = input.identifier
        val option = input.option.getOrElse("")
        logger.info(s"input.option $option")


        inputDataFrame = FileOperation.getFileDF(spark, "", path, option)
        SparkJob.createTempTable(spark,inputDataFrame,input.`df-name`)
      }
    }
    inputDataFrame
  }

  def createTransformation(transform: Transform, spark: SparkSession):  scala.collection.mutable.Map[String, String] = {

      //val tempTable = transform.t_inputs.getOrElse("")
      //println(s"select overs from ${tempTable} where batsman='S Dhawan'")
      val df_transform = spark.sql(s"${transform.query}")
      logger.info(s"Creating temp view ${transform.`df-name`}")
      df_transform.createOrReplaceTempView(s"${transform.`df-name`}")



      logger.info(s"transform.`df-name ${transform.`df-name`}")
      logger.info(s"transform.t_inputs ${transform.t_inputs.getOrElse("")}")
      logger.info(s"transform.query ${transform.query}")
      logger.info(s"transform.output ${transform.output}")

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

            logger.info(s"jdbc output.`df-name ${output.`df-name`}")
            logger.info(s"jdbc output.query ${output.`type`}")
            logger.info(s"jdbc output.output ${output.table}")
            logger.info(s"jdbc output.output ${output.schema}")
            logger.info(s"jdbc output.output ${output.identifier}")
            logger.info(s"file output.mode ${output.mode}")

            val connectionUrl = commonConfig.get("url")
            val userName = commonConfig.get("username")
            val password = commonConfig.get("password")
            println("connectionUrl=>"+connectionUrl)

            writeToJdbc(spark,f._1,output.`df-name`,
              output.schema.getOrElse(""),output.table.getOrElse(""),
              output.`type`,output.identifier,
              connectionUrl,userName,password,
              output.mode.getOrElse(""))
          }
        }
        )
      }

      // Check if output type is file
      case "file" => {
        transformToOutputMapping.foreach(f => {
          println(f._1, f._2)
          if (f._2 == output.`df-name`) {
            logger.info(s"file output.`df-name ${output.`df-name`}")
            logger.info(s"file output.t_inputs ${output.path}")
            logger.info(s"file output.query ${output.`type`}")
            logger.info(s"file output.output ${output.identifier}")
            logger.info(s"file output.output_format ${output.output_format}")
            logger.info(s"file output.option ${output.option}")
            logger.info(s"file output.mode ${output.mode}")
            /*output.output_format match {
              case "parquet" => println(output.output_format)
              case _ => println("No match")
            }*/
            writeToFile(spark,f._1,output.`df-name`,
              output.path.getOrElse(""),output.`type`,
              output.identifier,output.output_format.getOrElse(""),
              output.mode.getOrElse("")
              )
          }
        }
        )
      }
    }

  }


}
