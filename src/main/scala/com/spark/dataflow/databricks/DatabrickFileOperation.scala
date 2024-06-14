package com.spark.dataflow.databricks

import com.spark.dataflow.models.FileOperation.getClass
import com.spark.dataflow.utils.CommonCodeSnippet
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DatabrickFileOperation {

  val logger: Logger = LogManager.getLogger(getClass.getSimpleName)

  def getFileDF(path: String,
                more_option: String,
                format: String = "csv",
                delimiter: String = ","): String = {


    val arrayOfOption = more_option.split("\n")
    logger.info(arrayOfOption)

    var mapOfOption:String = ""
    for(element <- arrayOfOption){
      mapOfOption=mapOfOption+element.replace("=","='")+"',"
    }
   // val mapOfOption = arrayOfOption.map(s => (s"""${s.split("=")(0)}""", s"""\'${s.split("=")(1)}\'"""))

    logger.info("mapOfOption " + mapOfOption.init)

    s"""${CommonCodeSnippet.indentation}${CommonCodeSnippet.fileDf} = (spark.read
       |.format(\"${format}\")
       |.options(${mapOfOption.init})
       |.load(\"${path}\")
       |)""".stripMargin
  }

}
