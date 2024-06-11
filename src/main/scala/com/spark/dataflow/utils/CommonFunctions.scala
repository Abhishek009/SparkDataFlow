package com.spark.dataflow.utils
import com.spark.dataflow.models.FileOperation.logger
import org.apache.spark.unsafe.array.ByteArrayMethods

import java.io.{BufferedWriter, File, FileWriter}

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

    def writeToStaging(data:String,fileLocation:String,fileName:String) = {
        val file = new File(fileLocation+"/"+fileName)
        val dataWriter = data+System.lineSeparator();
        try{
            var fr = new FileWriter(file,true)
            var bw = new BufferedWriter(fr)
            bw.write(dataWriter)
            bw.close();
        }catch{
            case e:Exception => println(e.printStackTrace())
        }
    }

    def deleteFile(fileLocation:String,fileName:String): Unit = {
        val file = new File(fileLocation+"/"+fileName);

        if (file.delete()) {
            println("File deleted successfully");
        }
        else {
            println("Failed to delete the file");
        }
    }


    }
