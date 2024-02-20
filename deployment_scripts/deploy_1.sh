#!/bin/bash

# Set default parameters
dir=$(pwd)
SOURCE_TEMPLATE="${dir}/consumption-patterns/emr/1_pyspark-streaming/cfn_template.yaml"
SUFFIX=$(date +%s)
KINESIS_STREAM_NAME="default-data-stream-${SUFFIX}"
STACK_NAME="blog-pyspark-realtime-${SUFFIX}"
API_KEY="api_key-${STACK_NAME}"
DEPENDENCIES=(python3 aws)
declare -i var OPTIONS_FOUND
OPTIONS_FOUND=0


# Fetch Attributes
while getopts ":a:s:e:p:r::x:" opt; do
  case $opt in
    a) ARTIFACT_BUCKET="$OPTARG" OPTIONS_FOUND+=1
    ;;
    s) SUBNET_ID="$OPTARG" OPTIONS_FOUND+=1
    ;;
    p) AWS_PROFILE="$OPTARG" OPTIONS_FOUND+=1
    ;;
    r) AWS_REGION="$OPTARG" OPTIONS_FOUND+=1
    ;;
    e) EP_URL="$OPTARG" OPTIONS_FOUND+=1
    ;;
    x) API_SECRET="$OPTARG" OPTIONS_FOUND+=1
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
    exit 1
    ;;
    :) echo "Option -$OPTARG requires an argument." >&2
    exit 1
    ;;
  esac
done

if ((OPTIONS_FOUND !=6)); then
  echo "Please make sure to pass all the required options \"-a -s -p -r -e -x\""
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

#1. Generate Spark application YAML configuration file
cd ${dir}/helper_scripts
python3 ./spark_parameters_realtime.py "${KINESIS_STREAM_NAME},${AWS_REGION},${EP_URL},${API_KEY},${OUTPUT_BUCKET}" ${dir}


#2. Upload scripts and configuration files to S3
cd ${dir}/consumption-patterns/emr/1_pyspark-streaming
aws s3 cp ./bootstrap_actions/bootstrap.sh s3://${ARTIFACT_BUCKET}/ --profile ${AWS_PROFILE}
aws s3 cp ./emr_steps/pyspark_realtime_pseudo.py s3://${ARTIFACT_BUCKET}/ --profile ${AWS_PROFILE}
aws s3 cp ./emr_steps/pyspark_realtime_config.yaml s3://${ARTIFACT_BUCKET}/ --profile ${AWS_PROFILE}
rm ./emr_steps/pyspark_realtime_config.yaml

#3. Create the secret in Secrets Manager to store the REST API secret key
SECRET_STRING=$(cat <<EOF
    { "api_key_value": "${API_SECRET}" }
EOF
)

KEY_ARN=$(aws secretsmanager create-secret \
    --name ${API_KEY} \
    --description "REST API Secret Key" \
    --query ARN \
    --output text \
    --region ${AWS_REGION} \
    --secret-string "${SECRET_STRING}")


#4. Deploy the CloudFormation Stack to the configured AWS Account from the generated template
cd ${dir}
aws cloudformation deploy --template-file ${SOURCE_TEMPLATE} \
    --stack-name ${STACK_NAME} \
     --parameter-overrides ArtifactBucketName=${ARTIFACT_BUCKET} LogBucketName=${LOG_BUCKET} OutputBucketName=${OUTPUT_BUCKET} SubnetId=${SUBNET_ID} ApiKeyArn=${KEY_ARN} KinesisStream=${KINESIS_STREAM_NAME} \
     --capabilities CAPABILITY_IAM \
     --stack-name ${STACK_NAME} \
     --region ${AWS_REGION}

#5. Print the CloudFormation Stack output values
aws cloudformation  describe-stacks --stack-name ${STACK_NAME} \
     --query "Stacks[0].Outputs" --output table \
     --region ${AWS_REGION}

EMR_CLUSTER_ID=$(aws cloudformation  describe-stacks --stack-name ${STACK_NAME} --query "Stacks[0].Outputs[?OutputKey=='EMRClusterId'].OutputValue" --output text --region ${AWS_REGION})

echo """
  To simulate a Kinesis Producer, run the following command:
  python3 consumption-patterns/emr/1_pyspark-streaming/kinesis_producer/producer.py ${KINESIS_STREAM_NAME} 50 1

  To cleanup the AWS resources, run the following command:
  sh deployment_scripts/cleanup_1.sh -a ${ARTIFACT_BUCKET} -s ${STACK_NAME} -r ${AWS_REGION} -e ${EMR_CLUSTER_ID}
"""
