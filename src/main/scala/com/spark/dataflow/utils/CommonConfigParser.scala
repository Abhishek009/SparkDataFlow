package com.spark.dataflow.utils


import com.spark.dataflow.Flow.getClass
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import upickle.default.read

import scala.io
import java.io.{File, FileInputStream, FileNotFoundException}
import java.util
import java.util.Properties
import scala.collection.JavaConverters._
import scala.io.Source



object CommonConfigParser {

  val logger = LogManager.getLogger(getClass.getSimpleName)

  /**
   *
   * @param configFile
   * @param connectionType
   * @param connectionSubType
   * @return Map[String,String]
   */
  def parseConfig(configFile: String, connectionType: String, connectionSubType: String): util.Map[String, String] = {


    val jobConfig = new FileInputStream((new File(configFile)))
    val yamlData = new Yaml().load(jobConfig).asInstanceOf[java.util.Map[String, Any]]
    val jdbcConfiguration = yamlData.get(connectionType).asInstanceOf[java.util.Map[String, String]]
    jdbcConfiguration.get(connectionSubType).asInstanceOf[java.util.Map[String, String]]

  }

  def getMetaConfig(configVariables:String):Map[String,String] ={

    val input = io.Source.fromFile(configVariables).mkString
    read[Map[String,String]](input)
  }

  def getSparkConfig(sparkConfigFile:String):Map[String,String] = {
    val lines = Source.fromFile(sparkConfigFile).getLines()
    val propertiesMap = lines.map {
      line =>
        val Array(key, value) = line.split("=")
        key -> value
    }.toMap
    propertiesMap
  }
}
