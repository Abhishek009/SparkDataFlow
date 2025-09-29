#!/bin/bash

# Initialize variables
CMD=""
JOB_FILE=""
CONFIG_FILE=""
SPARK_CONFIG_FILE=""

#export SDF_LINEAGE_PATH="/home/jovyan/work/SparkDataFlow"
#export SDF_BASE_PATH="/home/jovyan/work/SparkDataFlow"

#sh ${SDF_BASE_PATH}/src/run_sdf.sh --cmd run --j ${SDF_BASE_PATH}/yaml/job_fileToMysql.yml --s ${SDF_BASE_PATH}/sdf.conf --c ${SDF_BASE_PATH}/config.yml
#sh ${SDF_BASE_PATH}/src/run_sdf.sh --cmd generate --j ${SDF_BASE_PATH}/yaml/job_fileToMysql.yml

if [ -z "$SDF_BASE_PATH" ]; then
        echo "Error: SDF_BASE_PATH is empty or unset. "
        exit 1
    else
        echo "SDF_BASE_PATH has a value: $SDF_BASE_PATH"
    fi


# Parse flags
while [ $# -gt 0 ]; do
  case "$1" in
    --cmd)
      CMD="$2"
      shift 2
      ;;
    --j)
      JOB_FILE="$2"
      shift 2
      ;;
    --c)
      CONFIG_FILE="$2"
      shift 2
      ;;
    --s)
      SPARK_CONFIG_FILE="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1"
      echo "Usage:"
      echo "  $0 --cmd run --j <job_file> --c <config_file> --s <spark_config_file>"
      echo "  $0 --cmd generate --j <job_file>"
      exit 1
      ;;
  esac
done

# Validate command
case "$CMD" in
  run)
    if [[ -z "$JOB_FILE" || -z "$CONFIG_FILE" ]]; then
      echo "Missing job or config file for 'run' command"
      exit 1
    fi

    while IFS= read -r line
    do
      config="${config} --conf ${line}"
    done < "${SPARK_CONFIG_FILE}"


    spark-submit \
    --master local \
    --class com.spark.dataflow.Flow "${SDF_BASE_PATH}/jars/SparkDataFlow-jar-with-dependencies.jar" \
    --configFile "${CONFIG_FILE}" \
    --jobFile "${JOB_FILE}" \
    --jobConfig "${SPARK_CONFIG_FILE}"
    ;;
  generate)
    if [ -z "$JOB_FILE" ]; then
      echo "Missing job file or output path for 'generate' command"
      exit 1
    fi
    java -cp ${SDF_BASE_PATH}/jars/SparkDataFlow-jar-with-dependencies.jar \
    com.spark.dataflow.lineage.LineageGenerator "${JOB_FILE}" \
    "${SDF_LINEAGE_PATH}/ReactBuild/pipelineConfig.json"
    java -cp ${SDF_BASE_PATH}/jars/SparkDataFlow-jar-with-dependencies.jar \
    com.spark.dataflow.lineage.SpringBootStaticFileServer \
    --external.static.files.path="${SDF_LINEAGE_PATH}/ReactBuild" --server.port=8000
    ;;
  *)
    echo "Invalid or missing --cmd value. Use 'run' or 'generate'."
    exit 1
    ;;
esac