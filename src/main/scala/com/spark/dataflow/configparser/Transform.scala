package com.spark.dataflow.configparser

case class Transform(
                      `df-name`: String,
                      t_inputs: Option[String],
                      query: String,
                      output: String)
