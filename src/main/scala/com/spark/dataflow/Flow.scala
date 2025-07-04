package com.spark.dataflow

import com.spark.dataflow.configparser.{Input, Output, Pipeline, Transform}
import com.spark.dataflow.databricks.{DatabricksEngine, DatabricksFlowOperation}
import org.apache.logging.log4j.LogManager
import org.yaml.snakeyaml.Yaml
import scala.collection.convert.ImplicitConversions.`map AsScala`
import com.spark.dataflow.models.FlowOperation
import com.spark.dataflow.utils.CommonCodeSnippet._
import com.spark.dataflow.utils.{CommonCodeSnippet, CommonConfigParser, CommonFunctions, SparkJob}
import org.apache.spark.sql.{DataFrame, DatasetShims}
import java.io.{File, FileInputStream}
import java.util
import scala.collection.JavaConverters._
/*
Scala SDK 2.12.15
--configFile "D:\Google_Drive_Rahul\java_program\BigData\SparkDataFlow\src\main\resources\job.yml"
--jobFile "D:\\Google_Drive_Rahul\\java_program\\BigData\\SparkDataFlow\\src\\main\\resources\\config.yml"
*/

object Flow extends DatasetShims {

  def main(args: Array[String]): Unit = {

    var inputMap:DataFrame = null;
    var usage:String =
      """Looks like you have pass some yaml key which is not acceptable by the SparkDataFlow
        | Acceptable tags are input,transform,output""".stripMargin

    val logger = LogManager.getLogger(getClass.getSimpleName)
    val conf = new CLIConfigParser(args)
    val argsMap: scala.collection.mutable.Map[String, String] = conf.getArgMap()
    val commonConfigFileName = argsMap.getOrElse("configFile","")
    val jobConfigFileName = argsMap.getOrElse("jobFile","")
    val configVariables = argsMap.getOrElse("jobConfig","")
    val jobConfig = new FileInputStream((new File(commonConfigFileName)))
    val yaml  = new Yaml().load(jobConfig).asInstanceOf[java.util.Map[String, Any]].asScala.toMap
    val jobName = yaml("jobName").asInstanceOf[String]

    logger.info(s"Job Name: ${jobName}")
    logger.info(s"Configuration File Name: ${jobConfigFileName}")
    
    val job1 = yaml("job").asInstanceOf[util.ArrayList[util.Map[String, Any]]].asScala.toList.map
    {    job => {
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
          var sql=""

          if(transformMap("query").asInstanceOf[String].toString.contains("--file"))
            {
              val sqlPath=transformMap("query").asInstanceOf[String].toString.split("--file")(1)
              CommonFunctions.templateExecution(sqlPath.trim(),configVariables)
              sql=CommonFunctions.readFileAsString(CommonCodeSnippet.stagingLocation+"/"+new File(sqlPath.trim()).getName)
            }
          else
          {
            sql=transformMap("query").asInstanceOf[String]

          }

          Transform(
            transformMap("df-name").asInstanceOf[String],
            Option(transformMap("t_inputs").asInstanceOf[String]),
            sql,
            //transformMap("query").asInstanceOf[String],
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
            Option(outputMap.getOrElse("output_format","parquet").asInstanceOf[String]),
            Option(outputMap.getOrElse("option", "").asInstanceOf[String]),
            Option(outputMap.getOrElse("mode", "overwrite").asInstanceOf[String]),
            Option(outputMap.getOrElse("partition", "").asInstanceOf[String])

          )
        }
      }
    }
    }

    val engine = yaml("engine").asInstanceOf[String]
    logger.info(s"Engine being used will be ${engine}")
    val parsedYaml = Pipeline(jobName, engine ,job1)
    val jobList = parsedYaml.job
    var transformToOutputMapping: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map.empty[String, String]

    engine match {
      case "databricks" => {
        DatabricksEngine.startProcessingForDatabricks(jobList,jobConfigFileName,usage)
      }

      case "spark" => {

        val spark = SparkJob.createSparkSession(jobName, "local")
        jobList.foreach {
          case input: Input => {
            FlowOperation.createInput(input, spark, jobConfigFileName)
          }
          case transform: Transform => {
            transformToOutputMapping = FlowOperation.createTransformation(transform, spark)
            val sparkTempCatalog: DataFrame = spark.sql(s"show tables")

            logger.info(s"Spark Temp Catalog ")
            //logger.info(new DatasetHelper(sparkTempCatalog).toShowString(20))
            sparkTempCatalog.foreach(f => {
              logger.info(f.get(f.fieldIndex("database")) + "|" + f.get(f.fieldIndex("tableName")))
            })
            logger.info(s"Transform To Output Mapping ${transformToOutputMapping}")
          }

          case output: Output => {
            FlowOperation.createOutput(output, spark, transformToOutputMapping, jobConfigFileName)
          }
          case _ => {
            logger.error(usage)
          }
        }
      }
    }
  }
}
