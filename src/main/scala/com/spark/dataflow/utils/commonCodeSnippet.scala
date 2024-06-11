package com.spark.dataflow.utils

object commonCodeSnippet {

  val initialStatement=s"sparkSession = SparkSession.builder().getOrCreate()"
  val initialImports=
    s"""
       |import os
       |import pyspark.sql import SparkSession,DataFrame
       |""".stripMargin

  val mainFunction="if __name__ == '__main__':"
}
