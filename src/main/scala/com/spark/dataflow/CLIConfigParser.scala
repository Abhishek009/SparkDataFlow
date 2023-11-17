

package com.spark.dataflow

import org.rogach.scallop._
import org.rogach.scallop.exceptions.{Exit, Help, RequiredOptionNotFound, ScallopException}

import scala.collection.mutable.Map

class CLIConfigParser(arguments: Seq[String]) extends ScallopConf(arguments) {

  banner("""Usage: Job Configuration [OPTIONS] ...
|For SparkDataFlow to work there are few configuration which needs to be passed.
|Options:
""")

  val configFile = opt[String](
    name = "configFile",
    required = true,
    descr = "Config File is required")

  val jobFile = opt[String](
    name = "jobFile",
    required = true,
    descr = "Job File is required")

  def getArgMap(): Map[String, String] = {
    var argsMap = scala.collection.mutable.Map[String, String]()
    argsMap += ("configFile" -> configFile())
    argsMap += ("jobFile" -> jobFile())
    /*argsMap += ("date" -> date())
    argsMap += ("ctlId" -> ctlId())
    argsMap += ("groupId" -> groupId())
    argsMap += ("transformType" -> transformType())
    argsMap += ("source" -> source())*/
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




