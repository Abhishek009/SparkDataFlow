package com.spark.dataflow.utils


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import scala.io
import java.io.{File, FileInputStream}
import java.util
import scala.collection.JavaConverters._



object CommonConfigParser {

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

  def getMetaConfig():Map[String,String] ={

    val map = io.Source.fromFile("resource/meta.conf").mkString.split("=").map(arr => (arr(0)->arr(1))).toMap
map
  }

}
