
import os
from pyspark.sql import SparkSession,DataFrame

if __name__ == '__main__':
	sparkSession
	df = sparkSession.read.table("ipl.deliveries")
	df.createOrReplaceTempView("dbread")
	fileDF = (spark.read
.format("csv")
.options(delimiter=',',header='true')
.load("D:/SampleData/IPL/matches.csv")
)
	fileDF.createOrReplaceTempView("books")
	t1=sparkSession.sql(""" Select * from dbread """)
	t1.write.mode("overwrite").saveAsTable("sparkdataflow.samplecustomerdata3")

import os
from pyspark.sql import SparkSession,DataFrame

if __name__ == '__main__':
	sparkSession
	df = sparkSession.read.table("ipl.deliveries")
	df.createOrReplaceTempView("dbread")
	fileDF = (spark.read
.format("csv")
.options(delimiter=',',header='true')
.load("D:/SampleData/IPL/matches.csv")
)
	fileDF.createOrReplaceTempView("books")
	t1=sparkSession.sql(""" Select * from dbread """)
	t1.write.mode("overwrite").saveAsTable(".")

import os
from pyspark.sql import SparkSession,DataFrame

if __name__ == '__main__':
	sparkSession
	df = sparkSession.read.table("ipl.deliveries")
	df.createOrReplaceTempView("dbread")
	fileDF = (spark.read
.format("csv")
.options(delimiter=',',header='true')
.load("D:/SampleData/IPL/matches.csv")
)
	fileDF.createOrReplaceTempView("books")
	t1=sparkSession.sql(""" Select * from dbread """)
	t1.write.mode("overwrite").saveAsTable(".")
