
import os
from pyspark.sql import SparkSession,DataFrame

if __name__ == '__main__':
	sparkSession = SparkSession.builder.getOrCreate()
	sparkSession.conf.set("spark.sql.sources.partitionOverwrite","dynamic")
	sparkSession.conf.set("spark.shuffle.compress","true")
	df = sparkSession.read.table("ipl.deliveries")
	df.createOrReplaceTempView("dbread")
	fileDF = (sparkSession.read
.format("csv")
.options(header='true',delimiter=',')
.load("D:/SampleData/IPL/matches.csv")
)
	fileDF.createOrReplaceTempView("fileread")
	t1=sparkSession.sql(""" Select * from dbread a LEFT JOIN fileread b on a.id=b.id where ods='2024-06-19' """)
	t1.write.partitionBy("ods").mode("overwrite").save("D:\SampleData\mysql_sample_data_1")
	t1.write.mode("overwrite").format("csv").saveAsTable("deliveries.iploutput")
