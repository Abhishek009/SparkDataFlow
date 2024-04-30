package com.spark.dataflow.models

import com.spark.dataflow.models.FlowOperation.getClass
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.spark.dataflow.outputFormat.{generic_format, iceberg_format}

object FileOperation {

  val logger: Logger = LogManager.getLogger(getClass.getSimpleName)

  def getFileDF(spark: SparkSession,
              connectionType: String,
              path: String,
              more_option: String,
              format: String = "csv",
              delimiter: String = ","): DataFrame = {
    ("Passing extra parameters for read " + more_option)

    val arrayOfOption = more_option.split("\n")

    val mapOfOption = arrayOfOption.map(s => (s.split("=")(0), s"""${s.split("=")(1)}""")).toMap
    logger.info("mapOfOption " + mapOfOption)

    val fileDf = spark.read
      .format(format)
      .options(mapOfOption)
      .load(path)
    fileDf.show()
    fileDf
  }

  def writeToFile(spark: SparkSession,tempView:String,
                  df_name:String,
                  path:String,
                  outputType:String,
                  identifier:String,
                  output_format:String,
                  mode:String
                 ): Unit = {
    logger.info(s"=====Output format is ${output_format}=====")
    val df = spark.table(s"${tempView}")
    output_format match {
      case "iceberg" => {
          logger.error(s"Iceberg format does not supports writing to file.")
      }
      case "csv" =>{
          generic_format.write(df,"csv",mode,path)
      }
      case "tsv" => {
        generic_format.write(df,"csv",mode,path)
      }
      case "parquet" => {
        generic_format.write(df,"csv",mode,path)
      }
    }

  }
}
