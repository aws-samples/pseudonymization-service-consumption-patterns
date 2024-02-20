from __future__ import print_function

import boto3
import json
import requests

from os import path
import sys
from pyspark.sql import SparkSession, Row
from pyspark import SparkContext, SparkFiles
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream
from pyspark.sql import functions as F

from itertools import chain

import yaml
from yaml.loader import SafeLoader


"""
	Title: PySpark Streaming App
	Description: 
		This code is responsible to read a Kinesis Data Stream, apply pseudonymization via API Gateway and save results to s3
		The code picks up the configuration values from the YAML file which is provided as final argument of the spark-submit command.

	Example usage: spark-submit --deploy-mode client --packages org.apache.spark:spark-streaming-kinesis-asl_2.12:3.1.3  s3://blog-artifacts/pyspark_realtime_pseudo.py s3://blog-artifacts/pyspark_realtime_pseudo.yaml 
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
        self.__action = endpoint_url.split("/")[4]
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


if __name__ == "__main__":
    app_name = "pyspark-realtime-pseudo"
    batch_window = 60

    sc = SparkContext(appName=app_name)
    spark = SparkSession(sc)
    ssc = StreamingContext(sc, batch_window)

    s3_config_path = sys.argv[1]
    config_filename = path.basename(s3_config_path)
    sc.addFile(s3_config_path)

    config = None
    with open(SparkFiles.get(config_filename)) as config_file:
        dict_config = yaml.load(config_file, Loader=SafeLoader)
        config = Configuration(dict_config)

    if config:
        s3_table_path = f"s3://{config.output_bucket_name}/realtime/warehouse/{config.db_name}/{config.table_name}"

        # Create Iceberg Table on the given S3 path
        spark.sql(f"CREATE DATABASE IF NOT EXISTS iceberg_catalog.{config.db_name}")
        spark.sql(f"USE {config.db_name}")
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {config.db_name}.{config.table_name} (
                dept_id string,
                s_vin string,
                example_ts string
            )
            USING iceberg
            location '{s3_table_path}'"""
        )

        pseudonymization = Pseudonymize(
            aws_region=config.kinesis_region,
            endpoint_url=config.endpoint_url,
            secret_name=config.secret_name,
            repartition_no=config.repartition_no,
            determinism=config.determinism,
        )

        stream_records = KinesisUtils.createStream(
            ssc,
            app_name,
            config.kinesis_stream_name,
            config.kinesis_endpoint_url,
            config.kinesis_region,
            InitialPositionInStream.LATEST,
            2,
        )

        # For debug purposes, print streamed records
        # stream_records.pprint()

        # pseudonymize data via REST API
        def pseudonymize(rdd, input_column):
            if not rdd.isEmpty():
                SparkSession.builder.getOrCreate()
                input_df = spark.read.json(rdd)
                real_column = f"real_{input_column}"
                pseudo_column = f"s_{input_column}"
                prepared_df = input_df.withColumn(real_column, F.col(input_column))

                # Perform pseudonymization
                pseudonymized_df = pseudonymization.get_dataframe(
                    prepared_df, [input_column]
                )
                pseudonymized_df = pseudonymized_df.withColumnRenamed(
                    input_column, pseudo_column
                )
                pseudonymized_df = pseudonymized_df.drop(real_column)

                # Write pseudonymized dataset with Iceberg table format in S3
                pseudonymized_df.writeTo(
                    f"{config.db_name}.{config.table_name}"
                ).tableProperty("format-version", "2").append()

        # apply pseudonymize fn for each RDD
        stream_records.foreachRDD(lambda rdd: pseudonymize(rdd, config.input_column))

        ssc.start()
        ssc.awaitTermination()
