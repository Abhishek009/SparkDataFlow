< #-- Template for reading a file in Spark using Scala -->
# Set the application name and file format
val spark = SparkSession.builder.appName("${app_name}").getOrCreate()
val df = spark.read.format("${file_format!}").options(options).load("file_path")