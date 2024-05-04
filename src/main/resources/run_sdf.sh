

sdf_config=$1
while IFS= read -r line
do
  config="${config} --conf ${line}"
done < "s{sdf_config}"

export base_path="Your path"
spark-submit \
--master yarn \
${config} \
--class com.spark.dataflow.Flow ${base_path}/SparkDataFlow-jar-with-dependencies.jar \
--configFile "${base_path}/job_hiveToFile.yml" \
--jobFile "${base_path}/config.yml"