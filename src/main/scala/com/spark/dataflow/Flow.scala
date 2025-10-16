package com.spark.dataflow

import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, DatasetShims}

object Flow extends DatasetShims {

  def main(args: Array[String]): Unit = {
    val usage: String =
      """Looks like you have pass some yaml key which is not acceptable by the SparkDataFlow
        | Acceptable tags are input,transform,output""".stripMargin

    val logger = LogManager.getLogger(getClass.getSimpleName)
    val conf = new CLIConfigParser(args)
    val argsMap: scala.collection.mutable.Map[String, String] = conf.getArgMap()
    val jobProcessFile = argsMap.getOrElse("jobFile", "")
    val jobConfigFile = argsMap.getOrElse("jobConfig", "")
    val configVariables = argsMap.getOrElse("configFile", "")
    val paramFiles = argsMap.getOrElse("paramFile","")

    logger.info(s"Job yaml file $jobProcessFile")
    logger.info(s"Job COnfiguration file: ${jobConfigFile}")
    logger.info(s"Configuration file: ${configVariables}")
    logger.info(s"Parameter File : ${jobConfigFile}")

    val pipeline = JobParser.parseJobConfig(jobProcessFile, paramFiles)
    EngineRunner.run(pipeline, configVariables, usage)

  }

}
