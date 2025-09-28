#!/bin/bash

# Initialize variables
CMD=""
JOB_FILE=""
CONFIG_FILE=""
SPARK_CONFIG_FILE=""

if [ -z "$SDF_BASE_PATH" ]; then
        echo "Error: SDF_BASE_PATH is empty or unset. "
        exit 1
    else
        echo "SDF_BASE_PATH has a value: $SDF_BASE_PATH"
    fi


# Parse flags
while [[ $# -gt 0 ]]; do
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
      echo "  $0 --cmd run --j <job_file> --c <config_file> --c <spark_config_file>"
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

    export base_path=${SDF_BASE_PATH}
    spark-submit.cmd \
    --master local \
    --class com.spark.dataflow.Flow "${base_path}/target/SparkDataFlow-jar-with-dependencies.jar" \
    --configFile "${CONFIG_FILE}" \
    --jobFile "${JOB_FILE}"
    ;;
  generate)
    if [[ -z "$JOB_FILE" ]]; then
      echo "Missing job file or output path for 'generate' command"
      exit 1
    fi
    java -cp ${SDF_BASE_PATH}/target/SparkDataFlow-jar-with-dependencies.jar \
    com.spark.dataflow.lineage.LineageGenerator "${JOB_FILE}" \
    "${SDF_BASE_PATH}/ReactBuild/pipelineConfig.json"
    java -cp ${SDF_BASE_PATH}/target/SparkDataFlow-jar-with-dependencies.jar \
    com.spark.dataflow.lineage.SpringBootStaticFileServer \
    --external.static.files.path="${SDF_BASE_PATH}/ReactBuild" --server.port=8000
    ;;
  *)
    echo "Invalid or missing --cmd value. Use 'run' or 'generate'."
    exit 1
    ;;
esac