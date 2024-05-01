package com.spark.dataflow.utils
import com.spark.dataflow.models.FileOperation.logger
import org.apache.spark.unsafe.array.ByteArrayMethods

object CommonFunctions {

    def getOptions(extraOptions:String):Map[String,String] = {

        var mapOfOption:Map[String,String] = Map()

        if(!extraOptions.isEmpty){
            val arrayOfOption = extraOptions.split("\n")
            mapOfOption = arrayOfOption.map(s => (s.split("=")(0), s"""${s.split("=")(1)}""")).toMap
            logger.info("mapOfOption " + mapOfOption)
        }
        mapOfOption
    }


    }
