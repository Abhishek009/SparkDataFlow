package com.spark.dataflow.utils

import freemarker.cache.FileTemplateLoader
import freemarker.template.{Configuration, TemplateExceptionHandler}
import org.apache.commons.io._
import org.apache.logging.log4j.LogManager

import scala.collection.JavaConverters._
import java.nio.file.{Files, Paths}
import java.io.{BufferedOutputStream, BufferedWriter, DataOutputStream, File, FileInputStream, FileOutputStream, FileWriter, StringWriter}
import java.nio.charset.StandardCharsets


object CommonFunctions {
    val logger = LogManager.getLogger(getClass.getSimpleName)

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

   /* def deleteFile(fileLocation:String,fileName:String): Unit = {
        val file = new File(fileLocation+"/"+fileName);

        if (file.delete()) {
            println("File deleted successfully");
        }
        else {
            println("Failed to delete the file");
        }
    }*/


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

    def createTempFileFromGiven(originalSqlFile: String):String = {

        val original = new File(originalSqlFile)
        val newFileName=original.getName
        val fileNameWithoutExtension = newFileName.substring(0,newFileName.lastIndexOf("."))+".ftl"
        val templateFile = new File(s"templates/${fileNameWithoutExtension}")
        logger.info(s"Template file ${templateFile}")
        FileUtils.copyFile(original,templateFile)

        templateFile.getParent
    }

    private def writeDataToFile(renderedSql: String, file: String) = {
        val filePath = new File(file)
        val newFileName=filePath.getName
        val path = Paths.get(newFileName.substring(0,newFileName.lastIndexOf("."))+"toExecute"+newFileName.substring(newFileName.lastIndexOf("."),newFileName.length));
        Files.write(path, renderedSql.getBytes());

    }


    def templateExecution(sqlFile:String): Unit = {
        val sourceFile = new File(sqlFile).getName
        val cfg = new Configuration(Configuration.VERSION_2_3_31)
        val templateBasePath = createTempFileFromGiven(sqlFile)
        logger.info(s"Template Base Path ${templateBasePath}")
        val fileTemplateLoader = new FileTemplateLoader(new File(templateBasePath))

        cfg.setTemplateLoader(fileTemplateLoader)
        cfg.setDefaultEncoding("UTF-8")
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER)

        var filename = sourceFile.substring(0,sourceFile.lastIndexOf("."))+".ftl"
        logger.info(s"======> ${filename}")
        val template = cfg.getTemplate(filename)
        val out = new StringWriter
        val someAttributes = CommonConfigParser.getMetaConfig()
        /*val someAttributes = Map(
          "ods" -> "20240619"
        )*/
        template.process(someAttributes.asJava, out)
        try{
            FileUtils.writeStringToFile(new File(s"${CommonCodeSnippet.stagingLocation}/${sourceFile}"),out.toString, StandardCharsets.UTF_8)
        }catch {
            case e:Exception => {
                logger.error("Not able to created the file")
            }
        }
        //println(mustacheFile)
        //engine.layout("D:\\Google_Drive_Rahul\\GitHub\\SparkDataFlow\\sql\\test.ftl", someAttributes)
    }


    def removeFile(tempCodeFile: String):Boolean = {
        var isFileDeleted=false
        try {
            FileUtils.delete(new File(tempCodeFile))
            logger.info(s"File to delete ${tempCodeFile}")
            isFileDeleted = true
        }catch {
            case e:Exception => {
                logger.error(s"Not able to remove the file ${e}")
            }
        }

        isFileDeleted
    }


    def readFileAsString(fileLocation:String):String = {
        var data=""
        try{
            data = FileUtils.readFileToString(new File(fileLocation), "UTF-8")
        }
        catch {
            case e:Exception => {logger.error(s"Not able to read the sql file ${e.printStackTrace()}")}
        }
       data
    }

}
