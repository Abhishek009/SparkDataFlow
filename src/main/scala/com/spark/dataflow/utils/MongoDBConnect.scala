package com.spark.dataflow.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MongoDBConnect {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Read Mongo")
      .master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/recommendation.movies_ratings")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/recommendation.movies_recommend")
      .getOrCreate()

    val ratings = spark.read.format("com.mongodb.spark.sql.DefaultSource")
      .option("database", "recommendation")
      .option("collection", "movies_ratings")
      .load()

    ratings.show()
    ratings.printSchema()

    val output = ratings.where(col("rating") >= 4)
    println("Count: " + output.count())

    output.write.format("com.mongodb.spark.sql.DefaultSource")
      .mode("Overwrite")
      .option("database", "recommendation")
      .option("collection", "movies_recommend")
      .save()

  }



}
