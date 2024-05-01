package com.spark.dataflow.outputFormat

import com.spark.dataflow.models.FileOperation.logger
import com.spark.dataflow.utils.CommonFunctions
import org.apache.spark.sql.DataFrame

object generic_format {


  def write(df: DataFrame,
            output_format:String,
            mode: String,
            path: String,extraOptions:String) = {
    try{
      df.write
        .format(output_format)
        .options(CommonFunctions.getOptions(extraOptions))
        .mode(mode)
        .save(path)
    }
    catch {
      case e:Exception => {e.printStackTrace()}
    }
  }


}
