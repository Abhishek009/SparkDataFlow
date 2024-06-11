package com.spark.dataflow.configparser

case class Pipeline(
                     jobName: String,
                     engine:String,
                     job: List[Any])
