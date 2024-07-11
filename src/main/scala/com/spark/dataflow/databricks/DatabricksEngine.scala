package com.spark.dataflow.databricks


import com.spark.dataflow.configparser.{Input, Output, Transform}
import com.spark.dataflow.databricks
import com.spark.dataflow.databricks.Connection.getConfig
import com.spark.dataflow.utils.CommonCodeSnippet.{indentation, initialImports, mainFunction, sparkConfig, sparkSession, sparkSessionInitialize, stagingLocation, tempCodeFile}
import com.spark.dataflow.utils.{CommonConfigParser, CommonFunctions}
import com.spark.dataflow.utils.CommonFunctions.readFileAsString
import org.apache.logging.log4j.LogManager

import java.io.FileReader
import java.util.Properties
import scala.io.Source

object DatabricksEngine {
  val logger = LogManager.getLogger(getClass.getSimpleName)
  def startProcessingForDatabricks(jobList: List[Any], jobConfigFileName: String, usage: String): Unit = {

    logger.info("Inside databricks")
    var transformToOutputMapping: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map.empty[String, String]
    val databrickJobConfig = getConfig(jobConfigFileName,"dev")
    val sparkConfigs = databrickJobConfig.get("spark_config").getOrElse("")

    CommonFunctions.removeFile(s"${stagingLocation}/${tempCodeFile}")
    CommonFunctions.writeToStaging(initialImports,stagingLocation,tempCodeFile)
    CommonFunctions.writeToStaging(mainFunction,stagingLocation,tempCodeFile)
    CommonFunctions.writeToStaging(indentation+sparkSessionInitialize,stagingLocation,tempCodeFile)

    if (!sparkConfigs.isEmpty){
      val propertiesMap=CommonConfigParser.getSparkConfig(sparkConfigs)
      if (!propertiesMap.isEmpty){
        for (keys <- propertiesMap.keys ){
          CommonFunctions.writeToStaging(indentation+sparkConfig+s"""(\"$keys\",\"${propertiesMap.get(keys).getOrElse("")}\")""",stagingLocation,tempCodeFile)
        }
      }
    }
    var inputDatabricksIdentifier=""

    jobList.foreach(
      job => {
        job match {
          case input: Input => {
            val codeInput = DatabricksFlowOperation.createInput(input,jobConfigFileName)
            if(input.`type`.equalsIgnoreCase("databricks")) inputDatabricksIdentifier=input.identifier
            CommonFunctions.writeToStaging(codeInput,stagingLocation,tempCodeFile)
          }
          case transform: Transform => {
            transformToOutputMapping = DatabricksFlowOperation.createTransformation(transform)
          }
          case output: Output =>{
            DatabricksFlowOperation.createOuput(output,transformToOutputMapping,jobConfigFileName)
          }
          case _ => {
            logger.error(usage)
          }
        }
      })
    logger.info("Trying to get the cluster details and status")
    if(databricks.Connection.getCluster(jobConfigFileName,inputDatabricksIdentifier)){
      logger.info("Trying to create a temp dir in workspace")
      if(databricks.Connection.createTempDirInWorkspace(jobConfigFileName,inputDatabricksIdentifier)){
        logger.info("Directory was created succesfully")
        if(databricks.Connection.uploadTheFileToWorkspace(jobConfigFileName,inputDatabricksIdentifier)){
          logger.info("Code update succesfully")
          if(databricks.Connection.execute(jobConfigFileName,inputDatabricksIdentifier)){
            logger.info("Job execution completed")
          }
        }
      }
    }
  }



}
