import sys
import yaml
import logging
import os

# Setup logging
logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stderr, level=logging.INFO)


base = sys.argv[2]
base_url = f"{base}/consumption-patterns/emr/2_pyspark-batch/emr_steps"
input_file = "pyspark_batch_pseudo.yaml"
output_file = "pyspark_batch_config.yaml"


# Parse arguments
input = sys.argv[1]
input_list = input.split(",")
aws_region, endpoint_url, api_key, secret_name, input_uri = input_list
input_bucket, input_key = input_uri.replace("s3://", "").split("/", 1)
output_bucket_name = f"{input_bucket}-output"


# Main Function
def main():
    logger.info(f"Loading data from {base_url}/{input_file}")
    # Read config file and fill parameters
    f = open(f"{base_url}/{input_file}")

    config_file = yaml.full_load(f)
    config_file["aws_region"] = aws_region
    config_file["endpoint_url"] = endpoint_url
    config_file["secret_name"] = api_key
    config_file["input_s3_path"] = input_uri
    config_file["output_bucket_name"] = output_bucket_name

    f.close()

    logger.info(f"Dumping data to  {base_url}/{output_file}")
    # Write to output
    with open(f"{base_url}/{output_file}", "w") as outfile:
        yaml.dump(config_file, outfile, default_flow_style=False)
    logger.info(f"Job completed!")
    sys.exit(0)


if __name__ == "__main__":
    main()
