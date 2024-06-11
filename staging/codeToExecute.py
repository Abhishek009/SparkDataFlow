
import os
import pyspark.sql import SparkSession,DataFrame

if __name__ == '__main__':
	sparkSession = SparkSession.builder().getOrCreate()
	df = spark.read.table(f""YOUR_SCHEMA_NAME.etl_framework_source_one"")
	df.createOrReplaceTempView(etl_framework_source_one)
	df_transform = spark.sql("Select * from etl_framework_source_one")
	df_transform.createOrReplaceTempView(f"t1")

df.write
        .format("csv")
        .options("Map(delimiter -> \t)")
        .mode(append)
        .save(/your/hdfs/location)
