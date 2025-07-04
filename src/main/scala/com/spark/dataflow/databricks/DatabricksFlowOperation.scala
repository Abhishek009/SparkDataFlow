package com.spark.dataflow.databricks

import com.spark.dataflow.configparser.{Input, Output, Transform}
import com.spark.dataflow.utils.{CommonCodeSnippet, CommonFunctions}
import org.apache.logging.log4j.{LogManager, Logger}
import scala.collection.mutable

object DatabricksFlowOperation {

  var transformToOutputMapping: scala.collection.mutable.Map[String, String]=
  scala.collection.mutable.Map.empty[String, String]
  val logger: Logger = LogManager.getLogger(getClass.getSimpleName)

  def createTempTable(variableName:String,viewName:String): String = {
    s"""${CommonCodeSnippet.indentation}${variableName}.${CommonCodeSnippet.createOrTempView}(\"${viewName}\")"""
  }


  def createInput(input: Input, jobConfigFileName: String):String = {

    input.`type` match {

      case "file" => {
        logger.info(s"Input df-name ${input.`df-name`}")
        logger.info(s"Input type ${input.`type`}")
        logger.info(s"Data Read will happen from  ${input.`type`}")
        val path = input.path.getOrElse("")
        logger.info(s"Input path $path")
        val identifier = input.identifier
        val option = input.option.getOrElse("")
        logger.info(s"Input other option $option")
        val format= input.format.getOrElse("")
        logger.info(s"Input format is $format")
        val inputDataFrame = DatabrickFileOperation.getFileDF(path, option, format)
        val tempTable = createTempTable(CommonCodeSnippet.fileDf,input.`df-name`)
        inputDataFrame+System.lineSeparator()+tempTable

      }
      case "databricks" => {
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

        val inputDataFrame = s"""${CommonCodeSnippet.indentation}df = ${CommonCodeSnippet.sparkSession}.read.table(\"$schemaName.$tableName\")"""
        val tempTable = createTempTable("df",input.`df-name`)
        inputDataFrame+System.lineSeparator()+tempTable
      }
    }



  }

  def createTransformation(transform: Transform): mutable.Map[String, String] = {

    CommonFunctions.writeToStaging(s"""${CommonCodeSnippet.indentation}${transform.`df-name`}=${CommonCodeSnippet.sparkSession}.sql(\"\"\" ${transform.query.stripMargin} \"\"\")""","staging","codeToExecute.py")
    logger.info(s"Transform df-name ${transform.`df-name`}")
    logger.info(s"Transform t_inputs ${transform.t_inputs.getOrElse("")}")
    logger.info(s"Transform query ${transform.query}")
    logger.info(s"Checking if sql model is a inline query or a file")
    logger.info(s"Transform output ${transform.output}")

    transformToOutputMapping += s"${transform.`df-name`}" -> s"${transform.output}"
    transformToOutputMapping
  }

  def createOuput(output: Output, transformToOutputMapping: mutable.Map[String, String], jobConfigFileName: String): Unit =
  {
    output.`type` match {
      // Check if output.type is jdbc
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
            logger.info(s"File output.partition ${output.partition}")
            val output_format = if (output.output_format.getOrElse("").isEmpty) "" else s""".format(\"${output.output_format.getOrElse("")}\")""".trim
            val tempView = f._1
            val outputMode = output.mode.getOrElse("")
            val mapOfOption = CommonFunctions.getOptionsForDatabricks(output.option.getOrElse(""))
            val partition = if (output.partition.getOrElse("").isEmpty) "" else s""".partitionBy(\"${output.partition.getOrElse("")}\")""".trim
            CommonFunctions.writeToStaging(
              s"""${CommonCodeSnippet.indentation}${tempView}.write${output_format}${mapOfOption}${partition}.mode(\"${outputMode}\").save(\"${output.path.getOrElse("")}\")""".stripMargin, "staging", "codeToExecute.py")
          }
        })
      }
      case "databricks" => {
        transformToOutputMapping.foreach(f => {
          println(f._1, f._2)
          if (f._2 == output.`df-name`) {
            logger.info(s"Databricks output.df-name ${output.`df-name`}")
            logger.info(s"Databricks output.t_inputs ${output.path}")
            logger.info(s"Databricks output.type ${output.`type`}")
            logger.info(s"Databricks output.identifier ${output.identifier}")
            logger.info(s"Databricks output.output_format ${output.output_format}")
            logger.info(s"Databricks output.option ${output.option}")
            logger.info(s"Databricks output.mode ${output.mode}")
            logger.info(s"Databricks output.partition ${output.partition}")
            val tempView = f._1
            val outputTableName = output.table.getOrElse("")
            val outputSchemaName = output.schema.getOrElse("")
            val outputMode = output.mode.getOrElse("")
            val mapOfOption = CommonFunctions.getOptionsForDatabricks(output.option.getOrElse(""))
            val outputOptions = if (output.option.getOrElse("").isEmpty) "" else mapOfOption.trim
            val partition = if (output.partition.getOrElse("").isEmpty) "" else s""".partitionBy(\"${output.partition.getOrElse("")}\")""".trim
            CommonFunctions.writeToStaging(
              s"""${CommonCodeSnippet.indentation}${tempView}.write.mode(\"${outputMode}\")${outputOptions}${partition}.saveAsTable(\"$outputSchemaName.$outputTableName\")""".stripMargin, "staging", "codeToExecute.py")

          }
        })
      }
    }}

}
