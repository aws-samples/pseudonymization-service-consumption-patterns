import sys
import yaml
import logging
import os

# Setup logging
logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

base = sys.argv[2]
base_url = f"{base}/consumption-patterns/emr/1_pyspark-streaming/emr_steps"
input_file = "pyspark_realtime_pseudo.yaml"
output_file = "pyspark_realtime_config.yaml"


# Parse arguments
input = sys.argv[1]
input_list = input.split(",")
(
    kinesis_stream_name,
    kinesis_region,
    endpoint_url,
    secret_name,
    output_bucket_name,
) = input_list
kinesis_endpoint_url = f"https://kinesis.{kinesis_region}.amazonaws.com"


# Main Function
def main():
    logger.info(f"Loading data from {base_url}/{input_file}")
    # Read config file and fill parameters
    f = open(f"{base_url}/pyspark_realtime_pseudo.yaml")

    config_file = yaml.full_load(f)
    config_file["kinesis_stream_name"] = kinesis_stream_name
    config_file["kinesis_endpoint_url"] = kinesis_endpoint_url
    config_file["kinesis_region"] = kinesis_region
    config_file["endpoint_url"] = endpoint_url
    config_file["secret_name"] = secret_name
    config_file["output_bucket_name"] = output_bucket_name

    f.close()

    logger.info(f"Dumping data to  {base_url}/{output_file}")
    # Write to output
    with open(f"{base_url}/{output_file}", "w") as outfile:
        yaml.dump(config_file, outfile, default_flow_style=False)
    logger.info(f"Job completed!")


if __name__ == "__main__":
    main()
