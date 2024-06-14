package com.spark.dataflow.databricks

import com.fasterxml.jackson.databind.ObjectMapper
import com.spark.dataflow.utils.CommonConfigParser
import org.apache.logging.log4j.LogManager
import sttp.client3.httpclient.HttpClientSyncBackend
import sttp.client3.quick._

import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.{Files, Paths}
import java.util.Base64

object Connection {
  val logger = LogManager.getLogger(getClass.getSimpleName)

  def getErrorMessage(host: String, runId: String, TOKEN: String) {
    val backend = HttpClientSyncBackend()
    val responseOutput = basicRequest.get(
        uri"https://${host}/api/2.1/jobs/runs/get-output")
      .body(runId)
      .auth.bearer(TOKEN)
      .send(backend)

    responseOutput.body match {

      case Left(body) => {
        logger.error(s"===> Non-2xx response to GET with code ${responseOutput.code}:\n$body")
        val mapper = new ObjectMapper()
        val jsonObject = mapper.readTree(body.toString)
        logger.error(s"ERROR MESSAGE => ${jsonObject.get("message")}")
      }
      case Right(body) => {
        val mapper = new ObjectMapper()
        val jsonObject = mapper.readTree(body.toString)
        logger.error(jsonObject.get("error"))
        logger.error(jsonObject.get("error_trace").asText.replaceAll("\n", "\n"))
      }
    }
  }

  def getConfig(jobConfigFileName: String): Map[String, String] = {
    val commonConfig = CommonConfigParser.parseConfig(jobConfigFileName, "databricks", "dev")
    val host = commonConfig.get("host")
    val http_path = commonConfig.get("http_path")
    val TOKEN = commonConfig.get("token")
    var config: Map[String, String] = Map()
    config += "host" -> host
    config += "http_path" -> http_path
    config += "TOKEN" -> TOKEN
    config
  }

  def getClusterId(http_path: String): String = {
    http_path.substring(http_path.lastIndexOf("/") + 1, http_path.length)
  }

  def checkJobStatus(runId: String, host: String, TOKEN: String): Boolean = {
    logger.info(s"Checking the job status for RunId $runId")
    var isJobCompleted = false
    var finalJobStatus = "NOTCOMPLETED"
    while (finalJobStatus != "COMPLETED") {
      val backend = HttpClientSyncBackend()
      val responseOutput = basicRequest.get(uri"https://${host}/api/2.1/jobs/runs/get")
        .body(runId).auth.bearer(TOKEN).send(backend)
      responseOutput.body match {
        case Left(body) => {
          logger.error(s"Non-2xx response to GET with code ${responseOutput.code}:\n$body")
          val mapper = new ObjectMapper()
          val jsonObject = mapper.readTree(body.toString)
          logger.error(s"ERROR MESSAGE ${jsonObject.get("message")}")
        }
        case Right(body) => {
          val mapper = new ObjectMapper()
          val jsonObject = mapper.readTree(body.toString)
          logger.info(s"Job with ${runId} is " + jsonObject.get("state")
            .get("life_cycle_state").asText())

          var joblifeCycleFinalState = jsonObject.get("state").get("life_cycle_state").asText()
          if (joblifeCycleFinalState == "TERMINATED") {
            if (jsonObject.get("state").get("result_state").asText() == "SUCCESS") {
              logger.info(s"Job with ${runId} is " + jsonObject.get("state").get("result_state").asText())
              finalJobStatus = "COMPLETED"
              isJobCompleted = true
            }
            if (jsonObject.get("state").get("result_state").asText() == "FAILED" || jsonObject.get("state").get("result_state").asText() == "CANCELED") {
              logger.info(s"Job with ${runId} is " + jsonObject.get("state").get("result_state").asText())
              finalJobStatus = "COMPLETED"
              getErrorMessage(host, runId, TOKEN)
              isJobCompleted = false
            }
          } else {
            Thread.sleep(2000)
          }
        }
      }
    }
    isJobCompleted
  }

  def checkTheStateOfCluster(json_message: String, host: String, TOKEN: String): Boolean = {

    logger.info("Check the status of the cluster")
    var isclusterRunning: Boolean = false
    val backend = HttpClientSyncBackend()
    while (!isclusterRunning) {
      val response = basicRequest.
        get(uri"https://${host}/api/2.0/clusters/get").body(json_message).auth.bearer(TOKEN).send(backend)


      val clusters = response.body.getOrElse("").toString
      response.body match {
        case Left(body) => {
          logger.info(s"===> Non-2xx response to GET With code ${response.code}:\n$body")
          val mapper = new ObjectMapper()
          val jsonObject = mapper.readTree(body.toString)
          println(jsonObject.get("message"))
        }
        case Right(body) => {
          println(s"===> 2xx response to GET with code ${response.code}\n$body ")
          val mapper = new ObjectMapper()
          val jsonObject = mapper.readTree(clusters)
          logger.info(s"Cluster is in ${jsonObject.get("state").asText()} state")
          if (jsonObject.get("state").asText() == "RUNNING") {
            isclusterRunning = true
          } else {
            Thread.sleep(2000)
          }
        }
      }
    }
    isclusterRunning
  }

  def startTheCluster(cluster_id: String, host: String, TOKEN: String): Boolean = {
    var isCusterStarted = false
    val json_message =
      s"""{"cluster_id": \"${cluster_id}\"}""".stripMargin

    val backend = HttpClientSyncBackend()
    val responseOutput = basicRequest.post(uri = uri"https://${host}/api/2.0/clusters/start").body(json_message).auth.bearer(TOKEN).send(backend)

    responseOutput.body match {
      case Left(body) => {
        logger.error(s"Non-2xx response to GET With code ${responseOutput.code}\n$body ")
        val mapper = new ObjectMapper()
        val jsonObject = mapper.readTree(body.toString)
        logger.error(s"ERROR MESSAGE ${jsonObject.get("message")}")
        isCusterStarted
      }
      case Right(body) => {
        logger.info(s"2xx response to GET with code ${responseOutput.code}:\n$body")
        if (checkTheStateOfCluster(json_message, host, TOKEN)) {
          isCusterStarted = true
        } else {
          isCusterStarted = false
        }
        isCusterStarted
      }
    }
  }

  def uploadTheFileToWorkspace(jobConfigFileName: String): Boolean = {
    var isCodeUpdated = false
    var clusterConfig = getConfig(jobConfigFileName)
    val http_path = clusterConfig.get("http_path").getOrElse("")
    val cluster_id = getClusterId(http_path)
    val host = clusterConfig.get("host").getOrElse("")
    val TOKEN = clusterConfig.get("TOKEN").getOrElse("")
    val path = Paths.get("/datadrive/athena_in_packages/common/scripts/sdf/codeToExecute.py")
    val text = new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
    logger.info(s"Original text data $text")
    val encodedText = Base64.getEncoder.encodeToString(text.getBytes(StandardCharsets.UTF_8))
    logger.info(s"encodedText text data $encodedText")
    val json_message =
      s"""f
        "path": "/workspace/Shared/direotoryViaAPI/v2/codeToExecute.py",
        "format": "SOURCE",
        "language": "PYTHON",
        "content": I"$encodedText\",
        "overwrite": true
        Į""".stripMargin
    val backend = HttpClientSyncBackend()
    val response = basicRequest.post(uri = uri"https://${host}/api/2.0/workspace/import").body(json_message).auth.bearer(TOKEN).send(backend)
    response.body match {
      case Left(body) => {
        logger.error(s"Non-2xx response to GET with code ${response.code}:\n$body")
        val mapper = new ObjectMapper()
        val jsonObject = mapper.readTree(body.toString)
        logger.error(jsonObject.get("message"))
      }
      case Right(body) => {
        logger.info(s"2xx response to GET with code ${response.code}")
        logger.info("Code is updated to databricks")
        isCodeUpdated = true
      }
    }
    isCodeUpdated
  }

  def getCluster(jobConfigFileName: String): Boolean = {
    logger.info(s"Getting the cluster info")
    var isClusterRunning = false
    var clusterConfig = getConfig(jobConfigFileName)
    val http_path = clusterConfig.get("http_path").getOrElse("")
    val cluster_id = getClusterId(http_path)
    val host = clusterConfig.get("host").getOrElse("")
    val TOKEN = clusterConfig.get("TOKEN").getOrElse("")
    val json_message = s"""{"cluster_id": \"$cluster_id\"}""".stripMargin
    logger.info(s"Host ${host}")
    logger.info(s"Http Path ${http_path}")
    logger.info(s"Getting the cluster info if cluster is running or stopped")
    val backend = HttpClientSyncBackend()
    val response = basicRequest
      .get(uri"https://${host}/api/2.0/clusters/get").body(json_message).auth.bearer(TOKEN).send(backend)
    // Process the response to get the list

    val clusters = response.body.getOrElse("").toString
    response.body match {
      case Left(body) => {
        logger.info(s"Non-2xx response to GET with code $response.codel:\n$body ")
        val mapper = new ObjectMapper()
        val jsonObject = mapper.readTree(body.toString)
        logger.error(jsonObject.get("message"))
        isClusterRunning = false
      }
      case Right(body) => {
        val mapper = new ObjectMapper()
        val jsonObject = mapper.readTree(clusters)
        if (jsonObject.get("cluster_id").asText() == cluster_id) {
          logger.info(jsonObject.get("cluster_name").asText() + " with "
            + jsonObject.get("cluster_id").asText() + " is in " + jsonObject.get("state").asText())
          if (jsonObject.get("state").asText() == "TERMINATED") {
            logger.info(s"Starting the cluster ${jsonObject.get("cluster_id").asText()}")
            if (startTheCluster(cluster_id, host, TOKEN)) {
              logger.info(s"${cluster_id} started successfully")
              isClusterRunning = true
            } else {
              logger.error(s"Fail to start the cluster")
              isClusterRunning = false
            }
          }
            else
            {
              if (checkTheStateOfCluster(json_message, host, TOKEN)) {
                isClusterRunning = true
              }
            }
          }
        }
      }
        isClusterRunning
    }

  def execute(jobConfigFileName: String):Boolean = {
    logger.info("Executing the code")

    val clusterConfig = getConfig(jobConfigFileName)
    val http_path = clusterConfig.get("http_path").getOrElse("")
    val cluster_id = getClusterId(http_path)
    val host = clusterConfig.get("host").getOrElse("")
    val TOKEN = clusterConfig.get("TOKEN").getOrElse("")
    val json_message =
      s"""
      {
    "run_name": "A multitask job run",
    "notebook_task":{
      "notebook_path": "/Workspace/shared/directoryViaAPI/v2/codeToExecute.py",
    "libraries":""
  },
  "existing_cluster_id": \"${cluster_id}\"
  }"""

    val backend = HttpClientSyncBackend()
    val responseSubmit = basicRequest.post(uri = uri"https://${host}/api/2.0/jobs/runs/submit")
      .body(json_message)
      .auth.bearer(TOKEN)
      .send(backend)

    var run_id = ""
    responseSubmit.body match {
      case Left(body) => {
        logger.error(s"Non-2xx response to GET with code ${responseSubmit.code}:\n$body")
        val mapper = new ObjectMapper()
        val jsonObject = mapper.readTree(body.toString)
        logger.error(s"ERROR MESSAGE => ${jsonObject.get("message")}")
      }
        case Right(body) => {
          logger.info(s"===> 2xx response to GET with code ${responseSubmit.code}")
          run_id = body.toString
        }
      }
        logger.info(s"===> $run_id")
        checkJobStatus(run_id, host, TOKEN)
    }

    def createTempDirInWorkspace(jobConfigFileName: String): Boolean = {
      var isDirectoryCreated = false
      val clusterConfig = getConfig(jobConfigFileName)
      val http_path = clusterConfig.get("http_path").getOrElse("")
      val cluster_id = getClusterId(http_path)
      val host = clusterConfig.get("host").getOrElse("")
      val TOKEN = clusterConfig.get("TOKEN").getOrElse("")
      val json_message = """{"path": "/Workspace/Shared/directoryViaAPI/v2/"}"""
      val backend = HttpClientSyncBackend()
      val response = basicRequest
        .post(uri = uri"https://${host}/api/2.0/workspace/mkdirs")
        .body(json_message).auth.bearer(TOKEN)
        .send(backend)
      response.body match {
        case Left(body) => {
          logger.error(s"===> Non-2xx response to GET with code ${response.code}:\n$body")
          val mapper = new ObjectMapper()
          val jsonObject = mapper.readTree(body.toString)
          logger.error(jsonObject.get("message"))
        }
        case Right(body) => {
          logger.info(s"===> 2xx response to GET")
          logger.info("Directory is created")
          isDirectoryCreated = true
        }
      }
        isDirectoryCreated
    }

  }



