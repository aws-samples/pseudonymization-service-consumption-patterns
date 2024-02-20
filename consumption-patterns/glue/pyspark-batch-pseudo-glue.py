import boto3
import json
import requests

from os import path
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession, Row
from pyspark.context import SparkContext
from pyspark import SparkFiles
from pyspark.sql import functions as F

from itertools import chain

import yaml
from yaml.loader import SafeLoader

"""
	Title: PySpark Batch App
	Description: 
		This code is responsible for reading a dataset from s3, apply pseudonymization via API Gateway and save pseudonomized dataset back to s3
		The code picks up the configuration values from the YAML file stored in s3, this file is created in during deployment.
"""


def get_credentials(id, region_name):
    sm_client = boto3.client("secretsmanager", region_name=region_name)
    string_result = sm_client.get_secret_value(SecretId=id)["SecretString"]
    dict_result = json.loads(string_result)
    return dict_result


class Pseudonymize:
    def __init__(
        self,
        aws_region,
        endpoint_url,
        determinism,
        secret_name,
        repartition_no,
    ):
        """
        Initialize class with parameters provided by the caller
        aws_region=
        endpoint_url=
        secret_name=
        repartition_no=
        """
        self.__aws_region = aws_region
        self.__endpoint_url = endpoint_url
        self.__action = endpoint_url.split("/")[-1]
        self.__secret_name = secret_name
        self.__repartition_no = repartition_no
        self.__endpoint = "{}?deterministic={}".format(endpoint_url, determinism)

    def __get_prepared_request(self):
        """
        Get Prepared Request
        """
        credentials = get_credentials(self.__secret_name, self.__aws_region)
        pr_request = requests.Request(
            method="POST",
            url=self.__endpoint,
            headers={"x-api-key": credentials["api_key_value"]},
        )
        return pr_request

    def __call_api(self, pseudonymize_columns, retry=0):
        """
        Make a call to the API based on the action requested by the caller.
        pseudonymisations or reidentifications
        """
        pr_request = self.__get_prepared_request()
        pseudonymize_columns = [str(col_val) for col_val in pseudonymize_columns]
        pseudonymize_columns_distinct = list(set(pseudonymize_columns))
        pseudonymize_columns_distinct = [
            i for i in pseudonymize_columns_distinct if i and str(i).strip() != ""
        ]
        result = ""
        if self.__action == "pseudonymization":
            pr_request.json = {"identifiers": pseudonymize_columns_distinct}
            final_request = pr_request.prepare()
            session = requests.Session()
            result = session.send(final_request).json().get("pseudonyms")
            if result is None or result == "None":
                if retry < 100:
                    return self.__call_api(pseudonymize_columns, retry + 1)
                else:
                    raise Exception(
                        "Failed inside pseudonymization lambda call. pseudonymize_columns_distinct : {}, result : {}, retry : {}".format(
                            pseudonymize_columns_distinct, result, retry
                        )
                    )
        else:
            pr_request.json = {"reidentifications": pseudonymize_columns_distinct}
            final_request = pr_request.prepare()
            session = requests.Session()
            result = session.send(final_request).json().get("identifiers")
            if result is None or result == "None":
                if retry < 100:
                    return self.__call_api(pseudonymize_columns, retry + 1)
                else:
                    raise Exception(
                        "Failed inside reidentifications lambda call. pseudonymize_columns_distinct : {}, result : {}, retry : {}".format(
                            pseudonymize_columns_distinct, result, retry
                        )
                    )
        if type(pseudonymize_columns_distinct) != type([]) or type(result) != type([]):
            raise Exception(
                "Failed inside lambda call. pseudonymize_columns_distinct : {}, result : {}, retry : {}".format(
                    pseudonymize_columns_distinct, result, retry
                )
            )
        mapping_dict = dict(zip(pseudonymize_columns_distinct, result))
        final_pseudonymize_columns = [
            mapping_dict[i_d] if i_d in mapping_dict.keys() else ""
            for i_d in pseudonymize_columns
        ]
        return final_pseudonymize_columns

    def __do_pseudonyms(
        self, iterator, batch_size, column_names, original_columns_order
    ):
        def fun_row_change_column_value(row, column_name, column_value):
            d = row.asDict()
            OrderedRow = Row(*original_columns_order)
            d[column_name] = column_value
            output_row = OrderedRow(*[d[i] for i in original_columns_order])
            return output_row

        def call_lambda_combine_result(column, chunk):
            request_list = [row[column] for row in chunk]
            response_list = self.__call_api(request_list)
            return [
                fun_row_change_column_value(row, column, result)
                for row, result in zip(chunk, response_list)
            ]

        def multiple_column_pseudonyms(e_chunk, col_names):
            if len(col_names) > 0:
                ec = col_names[0]
                er_chunk = call_lambda_combine_result(ec, e_chunk)
                return multiple_column_pseudonyms(er_chunk, col_names[1:])
            else:
                return e_chunk

        chunk_it = [
            iterator[i : i + batch_size] for i in range(0, len(iterator), batch_size)
        ]
        pseudonymized_chunk_it = [
            multiple_column_pseudonyms(a_chunk, column_names) for a_chunk in chunk_it
        ]
        return list(chain.from_iterable(pseudonymized_chunk_it))

    def get_dataframe(self, data_frame, column_names):
        """
        The function takes 2 parameters. Dataframe and List of Columns that need to be pseudonymized.
        It returns a dataframe with pseudonymized data.
        """
        # init
        batch_size = 900
        repartition_count = self.__repartition_no
        spark = SparkSession.builder.getOrCreate()
        # input df column in order
        original_columns_order = data_frame.columns

        # map function
        def fun(iterator):
            yield self.__do_pseudonyms(
                list(iterator), batch_size, column_names, original_columns_order
            )

        original_schema = data_frame.schema
        processed_rdd = (
            data_frame.rdd.repartition(repartition_count)
            .mapPartitions(fun)
            .flatMap(lambda x: x)
        )

        output_df = spark.createDataFrame(processed_rdd, schema=original_schema).cache()
        return output_df


class Configuration(object):
    """
    Initialize Configuration class with values provided in object
    """

    def __init__(self, data):
        for name, value in data.items():
            setattr(self, name, self._wrap(value))

    def _wrap(self, value):
        if isinstance(value, (tuple, list, set, frozenset)):
            return type(value)([self._wrap(v) for v in value])
        else:
            return Configuration(value) if isinstance(value, dict) else value


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
SparkSession.builder.getOrCreate()


s3_config_path = getResolvedOptions(sys.argv, ["configuration"])["configuration"]
config_filename = path.basename(s3_config_path)
sc.addFile(s3_config_path)

config = None

# Read config file
with open(SparkFiles.get(config_filename)) as config_file:
    dict_config = yaml.load(config_file, Loader=SafeLoader)
    config = Configuration(dict_config)

# Read input csv file
df = spark.read.option("header", True).option("delimiter", ",").csv(config.input_loc)
real_column = f"real_{config.input_column}"
pseudo_column = f"s_{config.input_column}"
df = df.withColumn(real_column, F.col(config.input_column))

# Perform pseudonymization
pseudonymization = Pseudonymize(
    aws_region=config.aws_region,
    endpoint_url=config.endpoint_url,
    secret_name=config.secret_name,
    repartition_no=config.repartition_no,
    determinism=config.determinism,
)

pseudonymized = pseudonymization.get_dataframe(df, [config.input_column])
pseudonymized = pseudonymized.withColumnRenamed(config.input_column, pseudo_column)
pseudonymized = pseudonymized.drop(real_column)
pseudonymized.show(5, False)

# Write pseudonymized dataset to S3
pseudonymized.write.mode("append").parquet(config.output_loc)

job.commit()
