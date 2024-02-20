#!/bin/bash


DEPENDENCIES=(python3 aws)
declare -i var OPTIONS_FOUND
OPTIONS_FOUND=0
DYNAMODB_TABLE_NAME="pyspark-realtime-pseudo"
ICEBERG_DYNAMO_CHECKPOINT_TABLE_NAME="blogStreamGlueLockTable"
GLUE_DATABASE_NAME="blog_stream_db"

# Fetch Attributes
while getopts ":a:s:r:e:" opt; do
  case $opt in
    a) ARTIFACT_BUCKET="$OPTARG" OPTIONS_FOUND+=1
    ;;
    s) STACK_NAME="$OPTARG" OPTIONS_FOUND+=1
    ;;
    r) AWS_REGION="$OPTARG" OPTIONS_FOUND+=1
    ;;
    e) EMR_CLUSTER_ID="$OPTARG" OPTIONS_FOUND+=1
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
    exit 1
    ;;
    :) echo "Option -$OPTARG requires an argument." >&2
    exit 1
    ;;
  esac
done

if ((OPTIONS_FOUND !=4)); then
  echo "Please make sure to pass all the required options \"-a -s -r -e\""
  exit 1
fi

unset OPTIONS_FOUND

function check_dependencies_mac()
{
  dependencies=$1
  for name in ${dependencies[@]};
  do
    [[ $(which $name 2>/dev/null) ]] || { echo -en "\n$name needs to be installed. Use 'brew install $name'";deps=1; }
  done
  [[ $deps -ne 1 ]] || { echo -en "\nInstall the above and rerun this script\n";exit 1; }
}

function check_dependencies_linux()
{
  dependencies=$1
  for name in ${dependencies[@]};
  do
    [[ $(which $name 2>/dev/null) ]] || { echo -en "\n$name needs to be installed. Use 'sudo apt-get install $name'";deps=1; }
  done
  [[ $deps -ne 1 ]] || { echo -en "\nInstall the above and rerun this script\n";exit 1; }
}


## Check dependencies by OS
if [ "$(uname)" == "Darwin" ]; then
    check_dependencies_mac "${DEPENDENCIES[*]}"   
elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
    check_dependencies_linux "${DEPENDENCIES[*]}"
else
  echo "Only Mac and Linux OS supported, exiting ..."
  exit 1   
fi

#0. Set Parameters
OUTPUT_BUCKET="${ARTIFACT_BUCKET}-output"
LOG_BUCKET="${ARTIFACT_BUCKET}-logs"
API_KEY="api_key-${STACK_NAME}"

#1. Delete the CloudFormation Stack
aws cloudformation delete-stack --stack-name ${STACK_NAME} --region ${AWS_REGION}

#2. Delete the stored secret for REST API integration
aws secretsmanager delete-secret --secret-id ${API_KEY} --force-delete-without-recovery --region ${AWS_REGION}

#3. Delete the DynamoDB table created from PySpark to checkpoint data from Kinesis
aws dynamodb delete-table --table-name ${DYNAMODB_TABLE_NAME} --region ${AWS_REGION}

#4. Delete the S3 objects
aws s3 rm s3://${ARTIFACT_BUCKET}/bootstrap.sh
aws s3 rm s3://${ARTIFACT_BUCKET}/pyspark_realtime_pseudo.py
aws s3 rm s3://${ARTIFACT_BUCKET}/pyspark_realtime_config.yaml

get_emr_cluster_state () {
  emr_cluster_state=$(aws emr describe-cluster --cluster-id ${EMR_CLUSTER_ID} --query Cluster.Status.State --region ${AWS_REGION} |  tr -d '"')
  if [[ $emr_cluster_state == "TERMINATED" ]]
  then
    return 0
  else
    return 1
  fi
}

while ! get_emr_cluster_state
do
   echo "waiting for EMR cluster to terminate.."
   sleep 10
done

sleep 10

#5. Delete the S3 buckets
aws s3 rb s3://${OUTPUT_BUCKET} --force
aws s3 rb s3://${LOG_BUCKET} --force

#6. Delete DynamoDB table blogGlueLockTable
aws dynamodb delete-table --table-name ${ICEBERG_DYNAMO_CHECKPOINT_TABLE_NAME} --region ${AWS_REGION}

#7. Delete Glue Database blog_db
aws glue delete-database --name ${GLUE_DATABASE_NAME} --region ${AWS_REGION}