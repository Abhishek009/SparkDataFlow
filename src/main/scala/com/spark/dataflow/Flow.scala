package com.spark.dataflow

import com.spark.dataflow.configparser.{Input, Output, Pipeline, Transform}
import org.apache.logging.log4j.LogManager
import org.yaml.snakeyaml.Yaml
import scala.collection.convert.ImplicitConversions.`map AsScala`
import com.spark.dataflow.models.{FileOperation, FlowOperation, MysqlOperation}
import com.spark.dataflow.utils.{CommonConfigParser, HS2, SparkJob}
import org.apache.spark.sql.{DataFrame, SparkSession,DatasetShims}
import java.io.{File, FileInputStream}
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
    val jobConfig = new FileInputStream((new File(commonConfigFileName)))
    val yaml  = new Yaml().load(jobConfig).asInstanceOf[java.util.Map[String, Any]].asScala.toMap
    val jobName = yaml("jobName").asInstanceOf[String]
    logger.info(s"Job Name ${jobName}")
    val spark = SparkJob.createSparkSession(jobName, "local")


    val job1 = yaml("job").asInstanceOf[java.util.ArrayList[java.util.Map[String, Any]]].asScala.toList.map
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
              Option(inputMap.getOrElse("path", "").asInstanceOf[String])
            )
          }
          case "transform" => {
            val transformMap = job("transform").asInstanceOf[java.util.Map[String, Any]].asScala.toMap
            Transform(
              transformMap("df-name").asInstanceOf[String],
              Option(transformMap("t_inputs").asInstanceOf[String]),
              transformMap("query").asInstanceOf[String],
              transformMap("output").asInstanceOf[String]
            )
           }

          case "output" => {
            val outputMap = job("output").asInstanceOf[java.util.Map[String, Any]].asScala.toMap
            Output(
              outputMap("df-name").asInstanceOf[String],
              outputMap("type").asInstanceOf[String],
              outputMap("identifier").asInstanceOf[String],
              Option(outputMap.getOrElse("path", "").asInstanceOf[String]),
              Option(outputMap.getOrElse("table", "").asInstanceOf[String]),
              Option(outputMap.getOrElse("schema", "").asInstanceOf[String]),
              Option(outputMap.getOrElse("output_format","parquet").asInstanceOf[String]),
              Option(outputMap.getOrElse("option", "").asInstanceOf[String]),
              Option(outputMap.getOrElse("partition", "").asInstanceOf[String]),
              Option(outputMap.getOrElse("mode", "append").asInstanceOf[String])
            )
          }
        }
      }
    }

    val parsedYaml = Pipeline(jobName, job1)
    val jobList = parsedYaml.job

    var transformToOutputMapping: scala.collection.mutable.Map[String, String] =
      scala.collection.mutable.Map.empty[String, String]

    jobList.foreach(
      job => {
        job match {
          case input: Input => {
            FlowOperation.createInput(input, spark,jobConfigFileName)
          }
          case transform: Transform => {
            transformToOutputMapping = FlowOperation.createTransformation(transform, spark)
            val sparkTempCatalog:DataFrame = spark.sql(s"show tables")

            logger.info(s"Spark Temp Catalog ")
            logger.info(new DatasetHelper(sparkTempCatalog).toShowString(20))
            //sparkTempCatalog.foreach(f =>  {logger.info(f.get(f.fieldIndex("database"))+ "|"+ f.get(f.fieldIndex("tableName")))})
            logger.info(s"Transform To Output Mapping ${transformToOutputMapping}")
          }

          case output: Output =>{
            FlowOperation.createOutput(output,spark,transformToOutputMapping,jobConfigFileName)
          }
           case _ => {
             logger.error(usage)
           }
        }
        }
        )
  }
}
