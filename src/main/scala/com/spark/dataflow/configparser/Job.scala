package com.spark.dataflow.configparser

case class Job(
                input: Option[Input],
                transform: Option[Transform],
                output: Option[Output])

//case class Job(job_name: String, job: List[Either[Input, Either[Transform, Output]]])


