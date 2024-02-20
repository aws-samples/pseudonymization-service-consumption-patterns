# BlogPost PII Consumption Patterns


## How to build pseudonymization service on AWS to protect sensitive data - Part 2

In part one of this blog series, we saw how to build a pseudonymization microservice which would take
plain text sensitive data as input and return a pseudonymized response. The whole idea behind having
pseudonymization as service is to have a centralized control on how the pseudonymization process works
across teams within an organization. This way, we not only achieve a standard process to handle sensitive
data but also avoid building a custom solution for every team and user group. This also takes away any
complexity and expertise needed to understand and implement various compliance requirements from
development teams and analytical users, so they can focus on solving their actual business problem.
Having a decoupled service also makes this solution technology agnostic from consumption perspective.
This means, no matter which technology we are using to interact with our data, we can call this service to
pseudonymize sensitive data. In this second part of the blog series, we will show some of the common
consumer patterns to integrate and use the pseudonymization service. We will also cover how to restrict
users based on their privileges to access all or only parts of functionality offered by the pseudonymization
service, hence adding an extra layer of protection to our data.

## Consuming the pseudonymization service using Amazon EMR

The pseudonymization service can be used from Amazon EMR in both streaming and batch use cases.
The PySpark applications will execute a common function to pseudonymize a Spark dataframe.

----

### Streaming

This use case consists in:
- Reading AWS Kinesis Data Stream
- Peforming pseudonymization via REST API calls in micro batch
- Writing the pseudonymized dataframe as Iceberg table in Amazon S3 and AWS Glue Data Catalog

In order to deploy this use case, you need:
- An AWS account
- An IAM Principal with privileges to deploy the stack and related resources
- Amazon S3 bucket living on the same account and same region where the solution is to be deployed
- Python3

#### Deployment steps

Execute the following code from your command line:
```
cd <repository/root/folder>

sh deployment_scripts/deploy_1.sh \
-a ARTEFACT_S3_BUCKET \
-r AWS_REGION \
-s SUBNET_ID \
-p AWS_PROFILE \
-e EP_URL \
-x API_SECRET
````

The parameters are:
- ARTEFACT_S3_BUCKET: S3 bucket where the infrastructure code will be stored. (The bucket must be created in the same account and same region where the solution lives)
- AWS_REGION: AWS region where the solution will be deployed
- AWS_PROFILE: Named profile that will apply to the AWS CLI command
- SUBNET_ID: Subnet ID where the EMR cluster will be spun up to
- EP_URL: Endpoint URL of the pseudonymization service
- API_SECRET: API Gateway secret that will be stored in Secrets Manager

Above command will create following resources:
- 1 Kinesis Stream
- 1 KMS Key
- 1 EMR Cluster
- 1 IAM Instance Profile
- 2 IAM Roles
- 2 S3 Bucket
- 1 EMR Step

#### Test the solution

In order to verify that the stream of records from Kinesis Data Stream is pseudonymized into Amazon S3 as Iceberg table, you need to call the Kinesis Data Producer at the end of the successful deployment.

Execute the following code from your command line to start the producer:
```
cd <repository/root/folder>

python3 consumption-patterns/emr/1_pyspark-streaming/kinesis_producer/producer.py KINESIS_STREAM_NAME BATCH THREADS ITERATIONS
````

The parameters are:
- KINESIS_STREAM_NAME: AWS Kinesis Data Stream created via CloudFormation
- BATCH: batch size to reach before pushing records to Kinesis
- THREADS: parallelism of the producer
- ITERATIONS: how many records are produced for each execution loop

You can: 
1. view the result in the output S3 bucket
2. query in Athena the table "pseudo_table" under the database "blog_stream_db"

#### Clean up resources

To cleanup the AWS resources, run the following command:

```
cd <repository/root/folder>

sh deployment_scripts/cleanup_1.sh \
-a ARTEFACT_S3_BUCKET \
-s STACK_NAME \
-r AWS_REGION \ 
-k API_KEY \
-e EMR_CLUSTER_ID
```

The parameters are:
- ARTEFACT_S3_BUCKET: 3 bucket where the infrastructure code will be stored. (The bucket must be created in the same account and same region where the solution lives)
- STACK_NAME: CloudFormation stack name
- AWS_REGION: AWS region where the solution will be deployed
- API_KEY: AWS Secrets Manager key used to store the REST API Secret key
- EMR_CLUSTER_ID: EMR Cluster ID used for the consumption pattern, created via CloudFormation

----

### Batch

This use case consists in:
- Reading CSV files from Amazon S3 URI
- Peforming pseudonymization via REST API calls 
- Writing the pseudonymized dataframe as Iceberg table in Amazon S3 and AWS Glue Data Catalog

In order to deploy this use case, you need:
- An AWS account
- An IAM Principal with privileges to deploy the stack and related resources
- Amazon S3 bucket living on the same account and same region where the solution is to be deployed
- Python3

#### Deployment steps

Execute the following code from your command line:
```
cd <repository/root/folder>

sh deployment_scripts/deploy_2.sh \
-a ARTEFACT_S3_BUCKET \
-r AWS_REGION \
-s SUBNET_ID \
-p AWS_PROFILE \
-e EP_URL \
-x API_SECRET \
-i S3_INPUT_PATH
```

The parameters are:
- ARTEFACT_S3_BUCKET: S3 bucket where the infrastructure code will be stored. (The bucket must be created in the same account and same region where the solution lives)
- AWS_REGION: AWS region where the solution will be deployed
- AWS_PROFILE: Named profile that will apply to the AWS CLI command
- SUBNET_ID: Subnet ID where the EMR cluster will be spun up to
- EP_URL: Endpoint URL of the pseudonymization service
- API_SECRET: API Gateway secret that will be stored in Secrets Manager
- INPUT_S3_PATH: the S3 URI of the CSV file containing the input dataset (The bucket must be in the same account and same region where the solution lives)

Above command will create following resources:
- 1 KMS Key
- 1 EMR Cluster
- 1 IAM Instance Profile
- 2 IAM Roles
- 2 S3 Bucket
- 1 EMR Step

#### Test the solution

In order to verify that the input files are pseudonymized into Amazon S3 as Iceberg table, you can:
1. view the result in the output S3 bucket
2. query in Athena the table "pseudo_table" under the database "blog_batch_db"


#### Clean up resources

To cleanup the AWS resources, run the following command:

```
cd <repository/root/folder>

sh deployment_scripts/cleanup_2.sh -a ARTEFACT_S3_BUCKET \
-s STACK_NAME \
-r AWS_REGION \ 
-k API_KEY \
-e EMR_CLUSTER_ID
```

The parameters are:
- ARTEFACT_S3_BUCKET: S3 bucket where the infrastructure code will be stored. (The bucket must be created in the same account and same region where the solution lives)
- STACK_NAME: CloudFormation stack name
- AWS_REGION: AWS region where the solution will be deployed
- API_KEY: AWS Secrets Manager key used to store the REST API Secret key
- EMR_CLUSTER_ID: EMR Cluster ID used for the consumption pattern, created via CloudFormation

----

## Consuming the pseudonymization service using Amazon Athena
This usecase consists of:
- Creating an Athena UDF using Lambda function
- Using the Athena UDF to pseudonymize or reidentify sensitive data in a SQL query

In order to deploy this use case, you need:
- An AWS account
- An IAM Principal with privileges to deploy the stack and related resources
- AWS CLI installed in your development or deployment environment
- An S3 bucket in the same region and same AWS account where the solution is to be deployed
- Java 11 or higher

#### Deployment steps

Execute the following code from your command line:
```
cd <repository/root/folder>

chmod +x ./deployment_scripts/deploy_3.sh 

sh ./deployment_scripts/deploy_3.sh \
-s <STACK_NAME> \
-b <ARTEFACT_S3_BUCKET> \
-p <AWS_PROFILE> \
-r <AWS_REGION> \
-e <EP_URL> \
-x <API_SECRET>
```

The parameters are:
- STACK_NAME: A logical name for the CloudFormation stack 
- ARTEFACT_S3_BUCKET: S3 bucket name where the infrastructure code will be stored. (The bucket must be created in the same account and same region where the solution lives)
- AWS_REGION: AWS region where the solution will be deployed
- AWS_PROFILE: Named profile that will apply to the AWS CLI command
- EP_URL: Endpoint URL of the pseudonymization service
- API_SECRET – API Gateway secret that will be stored in Secrets Manager

Above command will create following resources:
- 1 secret in Secret Manager
- 1 KMS Key
- 2 IAM Policies
- 1 IAM Role
- 1 Lambda Function

#### Test the solution

In order to verify the solution please follow the steps below: 
1. Login in to your AWS console and go to Athena service
2. To test the pseudonymization process, please run following command the Athena query editor - 
```
USING EXTERNAL FUNCTION pseudonymize(col1 VARCHAR) RETURNS VARCHAR LAMBDA '<NAME OF LAMBDA FUNCTION>' SELECT pseudonymize('["HFW5636AZWOUJ2PAM","4H46H0SXAI3MM49S5"]');
```
3. Please run the following command to test the reidentification function - 
```
USING EXTERNAL FUNCTION reidentify(col1 VARCHAR) RETURNS VARCHAR LAMBDA '<NAME OF LAMBDA FUNCTION>' SELECT reidentify(["ODUxZTc1YTg1NzUyF93SzQSd3yQ0+XyFDEftHENuNr3+QaDsai6dCBCxxCHN","MzVmYzNkZDQwMzlmPGA+Y4dybvi5Q/+i6zxnLoFRhDpLlx241hfCAkgYbkCG"]);
```

#### Clean up resources

To cleanup the AWS resources, run the following command:
 
```
cd <repository/root/folder>

./deployment_scripts/cleanup_3.sh \
-s STACK_NAME \
-b ARTEFACT_S3_BUCKET \
-p AWS_PROFILE \ 
-r AWS_REGION
```

The parameters are:
- STACK_NAME: CloudFormation stack name
- ARTEFACT_S3_BUCKET: S3 bucket name where the infrastructure code will be stored. (The bucket must be created in the same account and same region where the solution lives)
- AWS_PROFILE: Named profile that will apply to the AWS CLI command
- AWS_REGION: AWS region where the solution will be deployed

----

