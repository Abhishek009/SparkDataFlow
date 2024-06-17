package com.spark.dataflow.utils

import org.apache.logging.log4j.{LogManager, Logger}
import java.io.{BufferedWriter, File, FileWriter}

object CommonFunctions {
    val logger: Logger = LogManager.getLogger(getClass.getSimpleName)
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



    def getOptionsForDatabricks(extraOptions: String) = {
        var mapOfOption:String = ""
        if(!extraOptions.isEmpty){
            val arrayOfOption = extraOptions.split("\n")
            logger.info(arrayOfOption)


            for(element <- arrayOfOption){
                mapOfOption=mapOfOption+element.replace("=","='")+"',"
            }
        }
        mapOfOption.init
    }


}
