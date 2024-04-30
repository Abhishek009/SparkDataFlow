package com.spark.dataflow.outputFormat

import org.apache.spark.sql.DataFrame

object generic_format {


  def write(df: DataFrame, output_format:String, mode: String, path: String) = {
    try{
      df.write.format(output_format).mode(mode).save(path)
    }
    catch {
      case e:Exception => {e.printStackTrace()}
    }
  }
}
