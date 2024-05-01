package com.spark.dataflow.outputFormat

import com.spark.dataflow.models.FileOperation.logger
import org.apache.spark.sql.DataFrame

object generic_format {


  def write(df: DataFrame,
            output_format:String,
            mode: String,
            path: String,extraOptions:String) = {
    try{
      val arrayOfOption = extraOptions.split("\n")
      val mapOfOption = arrayOfOption.map(s => (s.split("=")(0), s"""${s.split("=")(1)}""")).toMap
      logger.info("mapOfOption " + mapOfOption)
      df.write.format(output_format).options(mapOfOption).mode(mode).save(path)
    }
    catch {
      case e:Exception => {e.printStackTrace()}
    }
  }


}
