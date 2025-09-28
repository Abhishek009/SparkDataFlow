package com.spark.dataflow.configparser

case class Input(
                  `type`: String,
                  identifier: String,
                  `df-name`: String,
                  var table: Option[String],
                  var schema: Option[String],
                  var option: Option[String],
                  var path: Option[String],
                  var format: Option[String]
                ) {
  table = Option(table.getOrElse(""))
  schema = Option(schema.getOrElse(""))
  path = Option(path.getOrElse(""))
  option = Option(option.getOrElse(""))
  format = Option(format.getOrElse(""))


}


