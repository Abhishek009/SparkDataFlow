package com.spark.dataflow

import com.spark.dataflow.Flow.getClass
import com.spark.dataflow.configparser.{Input, Output, Pipeline, Transform}
import com.spark.dataflow.utils.{CommonCodeSnippet, CommonConfigParser, CommonFunctions}
import org.apache.logging.log4j.{LogManager, Logger}

import java.io.File
import java.util
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`map AsScala`

object JobParser {
  val logger: Logger = LogManager.getLogger(getClass.getSimpleName)

  def parseJobConfig(jobProcessFile: String, configVariables: String): Pipeline = {
    val yamlOpt = CommonConfigParser.parseYamlConfig(jobProcessFile)
    val yaml: Map[String, Any] = yamlOpt.getOrElse(Map.empty[String, Any])
    val jobName = yaml("jobName").asInstanceOf[String]
    val engine = yaml("engine").asInstanceOf[String]
    logger.info(s"Job Name: ${jobName}")



    val jobList = yaml("job").asInstanceOf[java.util.ArrayList[java.util.Map[String, Any]]]
      .asScala.toList.map { job =>
        job.keys.head match {
          case "input" => {
            val inputMap = job("input").asInstanceOf[java.util.Map[String, Any]].asScala.toMap
            Input(
              inputMap("type").asInstanceOf[String],
              inputMap("identifier").asInstanceOf[String],
              inputMap("df-name").asInstanceOf[String],
              Option(inputMap.getOrElse("table", "").asInstanceOf[String]),
              Option(inputMap.getOrElse("schema", "").asInstanceOf[String]),
              Option(inputMap.getOrElse("option", "").asInstanceOf[String]),
              Option(inputMap.getOrElse("path", "").asInstanceOf[String]),
              Option(inputMap.getOrElse("format", "").asInstanceOf[String])
            )
          }
          case "transform" => {
            val transformMap = job("transform").asInstanceOf[java.util.Map[String, Any]].asScala.toMap
            var sql = ""

            if (transformMap("query").asInstanceOf[String].toString.contains("--file")) {
              val sqlPath = transformMap("query").asInstanceOf[String].toString.split("--file")(1)
              CommonFunctions.templateExecution(sqlPath.trim(), configVariables)
              sql = CommonFunctions.readFileAsString(CommonCodeSnippet.stagingLocation + "/" + new File(sqlPath.trim()).getName)
            }
            else {
              sql = transformMap("query").asInstanceOf[String]
            }

            Transform(
              transformMap("df-name").asInstanceOf[String],
              Option(transformMap("t_inputs").asInstanceOf[String]),
              sql,
              transformMap("output").asInstanceOf[String]
            )
          }
          case "output" => {
            val outputMap = job("output").asInstanceOf[util.Map[String, Any]].asScala.toMap
            Output(
              outputMap("df-name").asInstanceOf[String],
              outputMap("type").asInstanceOf[String],
              outputMap("identifier").asInstanceOf[String],
              Option(outputMap.getOrElse("path", "").asInstanceOf[String]),
              Option(outputMap.getOrElse("table", "").asInstanceOf[String]),
              Option(outputMap.getOrElse("schema", "").asInstanceOf[String]),
              Option(outputMap.getOrElse("output_format", "parquet").asInstanceOf[String]),
              Option(outputMap.getOrElse("option", "").asInstanceOf[String]),
              Option(outputMap.getOrElse("mode", "overwrite").asInstanceOf[String]),
              Option(outputMap.getOrElse("partition", "").asInstanceOf[String])

            )
          }
        }
      }
    Pipeline(jobName, engine, jobList)
  }
}
