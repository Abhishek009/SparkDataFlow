

package com.spark.dataflow

import org.rogach.scallop._
import org.rogach.scallop.exceptions.{Exit, Help, RequiredOptionNotFound, ScallopException}

import scala.collection.mutable.Map

class CLIConfigParser(arguments: Array[String]) extends ScallopConf(arguments) {

  banner("""Usage: Job Configuration [OPTIONS] ...
|For SparkDataFlow to work there are few configuration which needs to be passed.
|Options:
""")

  val configFile = opt[String](
    name = "configFile",
    required = true,
    descr = "Config File contains the connection details")

  val jobFile = opt[String](
    name = "jobFile",
    required = true,
    descr = "Job file which has the yaml with input, transformation and load.")

  val jobConfig = opt[String](
    name = "jobConfig",
    required = false,
    descr = "Contains Spark Configuration")

  val paramFile = opt[String](
    name = "paramFile",
    required = false,
    descr = "File which contains the variable for the sql")


  def getArgMap(): Map[String, String] = {
    var argsMap = scala.collection.mutable.Map[String, String]()
    argsMap += ("configFile" -> configFile())
    argsMap += ("jobFile" -> jobFile())
    argsMap += ("jobConfig" -> jobConfig.toOption.getOrElse(""))
    argsMap += ("paramFile" -> paramFile.toOption.getOrElse(""))


    argsMap
  }

  verify()

  override def onError(e: Throwable): Unit = e match {
    case Help("") => printHelp()
    case Exit() => printHelp()
    case ScallopException(message) => {
      println(message)
      printHelp()
    }
    case RequiredOptionNotFound(message) => {
      println(message)
      printHelp()
    }
    case other => throw other
  }

}




