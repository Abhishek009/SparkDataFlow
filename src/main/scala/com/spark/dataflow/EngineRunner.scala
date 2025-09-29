package com.spark.dataflow

import com.spark.dataflow.configparser.{Input, Output, Pipeline, Transform}
import com.spark.dataflow.models.FlowOperation
import com.spark.dataflow.utils.SparkJob
import org.apache.logging.log4j.{LogManager, Logger}

object EngineRunner {
  val logger: Logger = LogManager.getLogger(getClass.getSimpleName)

  def run(pipeline: Pipeline, configVariables: String, usage: String): Unit = {
    val jobList = pipeline.job
    var transformToOutputMapping = scala.collection.mutable.Map.empty[String, String]
    pipeline.engine match {
      case "spark" =>
        val spark = SparkJob.createSparkSession(pipeline.jobName)
        jobList.foreach {
          case input: Input => FlowOperation.createInput(input, spark, configVariables)
          case transform: Transform => transformToOutputMapping = FlowOperation.createTransformation(transform, spark)
          case output: Output => FlowOperation.createOutput(output, spark, transformToOutputMapping, configVariables)
          case _ => logger.error(usage)
        }
      case "databricks" =>
      // Call DatabricksEngine logic
      case _ => logger.error("Supported engine is Spark")

    }
  }
}
