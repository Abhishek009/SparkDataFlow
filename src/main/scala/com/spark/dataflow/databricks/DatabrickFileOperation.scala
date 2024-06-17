package com.spark.dataflow.databricks

import com.spark.dataflow.models.FileOperation.{getClass, logger}
import com.spark.dataflow.outputFormat.generic_format
import com.spark.dataflow.utils.{CommonCodeSnippet, CommonFunctions}
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DatabrickFileOperation {

  val logger: Logger = LogManager.getLogger(getClass.getSimpleName)

  def getFileDF(path: String,
                more_option: String,
                format: String = "csv",
                delimiter: String = ","): String = {



   // val mapOfOption = arrayOfOption.map(s => (s"""${s.split("=")(0)}""", s"""\'${s.split("=")(1)}\'"""))
    val mapOfOption = CommonFunctions.getOptionsForDatabricks(more_option)
    logger.info("mapOfOption " + mapOfOption)

    s"""${CommonCodeSnippet.indentation}${CommonCodeSnippet.fileDf} = (spark.read
       |.format(\"${format}\")
       |.options(${mapOfOption})
       |.load(\"${path}\")
       |)""".stripMargin
  }

  def writeToFile(path:String,
                  outputType:String,
                  identifier:String,
                  output_format:String,
                  mode:String,options:String
                 ): Unit = {
    logger.info(s"=====Output format is ${output_format}=====")
    //val df = spark.table(s"${tempView}")

    val mapOfOption = CommonFunctions.getOptionsForDatabricks(options)


    s"""${CommonCodeSnippet.indentation}${CommonCodeSnippet.fileDf} = (spark.read
       |.format(\"${output_format}\")
       |.options(${mapOfOption})
       |.mode(\"${mode}\")
       |.save(\"${path}\")
       |)""".stripMargin
  }
}
