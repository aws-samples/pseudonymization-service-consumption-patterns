#!/bin/bash

OUTPUT_TEMPLATE="output.yaml"
dir=$(pwd)
DEPENDENCIES=(mvn aws)
declare -i var OPTIONS_FOUND
OPTIONS_FOUND=0


while getopts ":s:b:p:r:e:" opt; do
  case $opt in
    s) STACK_NAME="$OPTARG" OPTIONS_FOUND+=1
    ;;
    b) ARTEFACT_BUCKET="$OPTARG" OPTIONS_FOUND+=1
    ;;
    p) AWS_PROFILE="$OPTARG" OPTIONS_FOUND+=1
    ;;
    r) AWS_REGION="$OPTARG" OPTIONS_FOUND+=1
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
  echo "Please make sure to pass all the required options \"-s -b -p -r\""
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


#1. Clean the target dir
cd "${dir}/consumption-patterns/athena/"
mvn clean

#2. Remove the output yaml file
rm OUTPUT_TEMPLATE

#3. Delete the Cloudformation Stack
aws cloudformation delete-stack --stack-name ${STACK_NAME} --region ${AWS_REGION}

#4. Delete the S3 objects
aws s3 rm s3://${ARTEFACT_BUCKET}/*
