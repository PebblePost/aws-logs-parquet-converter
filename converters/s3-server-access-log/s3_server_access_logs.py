from __future__ import annotations

import argparse
import logging
import os
import re
import time
from datetime import datetime, timedelta
from typing import List, Any, Optional
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError
from dateutil import parser
from dateutil.parser import parse
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

# From https://aws.amazon.com/premiumsupport/knowledge-center/analyze-logs-athena/
# See Also https://docs.aws.amazon.com/AmazonS3/latest/dev/LogFormat.html
S3_ACCESS_LOG_PATTERN = re.compile(
    r"(?P<owner>\S+) (?P<bucket>\S+) (?P<time>\[[^]]*\]) (?P<ip>\S+) "
    r"(?P<requester>\S+) (?P<reqid>\S+) (?P<operation>\S+) (?P<key>\S+) "
    r'(?P<request>"[^"]*"|-) (?P<status>\S+) (?P<error>\S+) (?P<bytes>\S+) '
    r'(?P<size>\S+) (?P<totaltime>\S+) (?P<turnaround>\S+) (?P<referrer>"[^"]*"|-) '
    r'(?P<useragent>"[^"]*"|-) (?P<version>\S) (?P<host_id>\S+) '
    r"(?P<signature_version>\S+) (?P<cipher_suite>\S+) (?P<auth_type>\S+) "
    r"(?P<host_header>\S+) (?P<tls_version>\S+) "
    r"(?P<access_point_arn>\S+) (?P<acl_required>\S+)"
)


def _get_aws_creds() -> dict:
    """
    Pull and return AWS creds from the env if exists.  This is primarily for local testing, EMR will use the IAM Role.
    :return: aws_dict
    """
    aws_dict = {
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "region_name": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    }
    return aws_dict


def _start_spark() -> "SparkSession":
    """
    Creates or returns a active Spark session
    :return: SparkSession
    """
    spark = (
        SparkSession.builder
        #
        .appName("s3_server_side_log_compacter")
        .config("spark.speculation", "false")
        # Note: We want to use timestamp with millis for Parquet
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1")
        .getOrCreate()
    )
    spark._jsc.hadoopConfiguration().set(
        "mapreduce.fileoutputcommitter.algorithm.version",
        "2",
    )
    return spark


def get_s3a_paths(s3_client: boto3.client, bucket: str, prefix: str) -> List[str]:
    """
    Mutate S3 object keys into a fully qualified s3a URI
    :param s3_client: internal boto s3 client
    :param bucket: bucket name
    :param prefix: prefix to search
    :return: list of s3a URIs for each item
    """
    return [
        "s3a://{}/{}".format(bucket, key)
        for key in list_bucket_with_prefix(s3_client, bucket, prefix)
    ]


def list_bucket_with_prefix(s3_client: boto3.client, bucket: str, prefix: str) -> List[str]:
    """
    Paginated listing of contents within an S3 bucket prefix
    :param s3_client: internal boto s3 client
    :param bucket: bucket name
    :param prefix: prefix to search
    :return: list of items
    """
    token = None
    more_keys = True
    keys = []
    while more_keys:
        kwargs = {
            "Bucket": bucket,
            "Prefix": prefix,
        }
        if token is not None:
            kwargs["ContinuationToken"] = token

        try:
            response = s3_client.list_objects_v2(**kwargs)
        except ClientError as e:
            logging.exception(f"Error listing complete S3 objects: {e}")
            break

        # Safely access 'Contents' and handle cases where it's not present
        contents = response.get("Contents", [])
        keys.extend([content["Key"] for content in contents])

        token = response.get("NextContinuationToken", None)
        more_keys = token is not None
    return keys


def s3_read_file(aws_cred_dict, s3_path) -> List[Row]:
    """
    Given a s3 file, read each line and convert to PySpark row
    :param aws_cred_dict: internal aws creds dict
    :param s3_path: s3a path to file
    :return: List of PySpark rows
    """
    s3_client = boto3.client("s3", **aws_cred_dict)
    parse_result = urlparse(s3_path)
    bucket = parse_result.netloc
    key = parse_result.path[1:]
    stream = s3_client.get_object(Bucket=bucket, Key=key)["Body"]
    rows = []
    whole_file = stream.read().decode("utf-8")
    for line in whole_file.split("\n"):
        line = line.strip()
        if not line:
            continue
        result = parse_apache_log_line(line)
        if result:
            rows.append(result)
    return rows


def parse_s3_access_time(s) -> datetime | None:
    """
    Parse and convert the s3 event timestamp
    :param s: raw time string
    :return: datetime or None
    """
    try:
        s = s[s.find("[") + 1 : s.find(" ")]
        return parser.parse(s.replace(":", " ", 1))
    except Exception:
        return None


def parse_apache_log_line(logline) -> Row:
    """
    Given a log line, return a row containing the S3 Server Access Log
    :param logline:
    :return: Row
    """
    match = S3_ACCESS_LOG_PATTERN.search(logline)
    if match is None:
        return Row(
            bucket_owner=None,
            s3_bucket=None,
            request_time_string=None,
            remote_ip=None,
            requester=None,
            request_id=None,
            operation=None,
            key=None,
            request=None,
            http_status=None,
            error_code=None,
            bytes_sent=None,
            object_size=None,
            total_time=None,
            turn_around_time=None,
            referrer=None,
            user_agent=None,
            version_id=None,
            host_id=None,
            signature_version=None,
            cipher_suite=None,
            authentication_type=None,
            host_header=None,
            tls_version=None,
            access_point_arn=None,
            acl_required=None,
            error_line=logline,
        )
    return Row(
        bucket_owner=match.group("owner"),
        s3_bucket=match.group("bucket"),
        request_time_string=parse_s3_access_time(match.group("time")).isoformat(),
        remote_ip=match.group("ip"),
        requester=match.group("requester"),
        request_id=match.group("reqid"),
        operation=match.group("operation"),
        key=None if match.group("key") == "-" else match.group("key"),
        request=match.group("request"),
        http_status=None if match.group("status") == "-" else int(match.group("status")),
        error_code=None if match.group("error") == "-" else match.group("error"),
        bytes_sent=None if match.group("bytes") == "-" else int(match.group("bytes")),
        object_size=None if match.group("size") == "-" else int(match.group("size")),
        total_time=None if match.group("totaltime") == "-" else int(match.group("totaltime")),
        turn_around_time=(
            None if match.group("turnaround") == "-" else int(match.group("turnaround"))
        ),
        referrer=None if match.group("referrer") == '"-"' else match.group("referrer"),
        user_agent=None if match.group("useragent") == '"-"' else match.group("useragent"),
        version_id=None if match.group("version") == "-" else match.group("version"),
        host_id=None if match.group("host_id") == "-" else match.group("host_id"),
        signature_version=(
            None if match.group("signature_version") == "-" else match.group("signature_version")
        ),
        cipher_suite=None if match.group("cipher_suite") == "-" else match.group("cipher_suite"),
        authentication_type=None if match.group("auth_type") == "-" else match.group("auth_type"),
        host_header=None if match.group("host_header") == "-" else match.group("host_header"),
        tls_version=None if match.group("tls_version") == "-" else match.group("tls_version"),
        access_point_arn=(
            None if match.group("access_point_arn") == "-" else match.group("access_point_arn")
        ),
        acl_required=None if match.group("acl_required") == "-" else match.group("acl_required"),
        error_line=None,
    )


S3_ACCESS_LOG_OUTPUT_SCHEMA = StructType(
    [
        StructField("bucket_owner", StringType(), True),
        StructField("s3_bucket", StringType(), True),
        StructField("request_time_string", StringType(), True),
        StructField("remote_ip", StringType(), True),
        StructField("requester", StringType(), True),
        StructField("request_id", StringType(), True),
        StructField("operation", StringType(), True),
        StructField("key", StringType(), True),
        StructField("request", StringType(), True),
        StructField("http_status", IntegerType(), True),
        StructField("error_code", StringType(), True),
        StructField("bytes_sent", LongType(), True),
        StructField("object_size", LongType(), True),
        StructField("total_time", LongType(), True),
        StructField("turn_around_time", LongType(), True),
        StructField("referrer", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("version_id", StringType(), True),
        StructField("host_id", StringType(), True),
        StructField("signature_version", StringType(), True),
        StructField("cipher_suite", StringType(), True),
        StructField("authentication_type", StringType(), True),
        StructField("host_header", StringType(), True),
        StructField("tls_version", StringType(), True),
        StructField("access_point_arn", StringType(), True),
        StructField("acl_required", StringType(), True),
        StructField("error_line", StringType(), True),
    ]
)


class S3ServerSideLoggingRollup:
    def __init__(
        self,
        aws_account_id: Optional[int],
        lookback_days: int,
        aws_region: str,
        access_log_bucket: str,
        destination_log_bucket: str,
        destination_log_prefix: str,
        num_output_files: int,
        hive_formatted_folders: bool,
        num_parallelize: int,
        start_date: Optional[str] = None,
    ) -> None:
        """
        Initialize S3ServerSideLoggingRollup instance.

        :param aws_account_id: AWS Account ID or 0 to retrieve from STS.
        :param lookback_days: Number of days to look back for logs.
        :param aws_region: AWS region where the buckets are located.
        :param access_log_bucket: Bucket containing access logs.
        :param destination_log_bucket: Bucket to store processed logs. Use 'same' to use access_log_bucket.
        :param destination_log_prefix: Prefix for destination log files.
        :param num_output_files: Number of output files.
        :param hive_formatted_folders: Whether to format folders in Hive style.
        :param num_parallelize: Number of parallel processes.
        :param start_date: Optional start date for processing.
        """
        self._connect_to_s3()
        self._get_aws_account_id(aws_account_id)
        self.lookback_days = datetime.today() - timedelta(days=lookback_days)
        self.aws_region = aws_region
        self.access_log_bucket = access_log_bucket
        self.destination_log_bucket = (
            access_log_bucket if destination_log_bucket == "same" else destination_log_bucket
        )
        self.destination_log_prefix = destination_log_prefix
        self.hive_formatted_folders = hive_formatted_folders
        self.num_partitions = num_output_files
        self.num_parallelize = num_parallelize
        self.start_date = start_date

    def _connect_to_s3(self) -> None:
        """
        Create a boto3 S3 client using internal credentials.
        """
        try:
            aws_cred_dict = _get_aws_creds()
            self.s3_client = boto3.client("s3", **aws_cred_dict)
        except Exception as e:
            logging.critical(f"Error connecting to S3: {e}")
            raise

    def _get_aws_account_id(self, aws_account_id: Optional[int]) -> None:
        """
        Retrieve AWS Account ID from the STS client if not provided.

        :param aws_account_id: Account ID passed in CLI args or 0 to retrieve from STS.
        """
        try:
            if aws_account_id == 0:
                aws_cred_dict = _get_aws_creds()
                sts_client = boto3.client("sts", **aws_cred_dict)
                response = sts_client.get_caller_identity()
                self.aws_account_id = response["Account"]
            else:
                self.aws_account_id = aws_account_id
        except Exception as e:
            logging.critical(f"Error getting AWS account ID: {e}")
            raise

    def get_list_of_folders(self) -> List[str]:
        """
        Return a list of folders using date-based partitioning.
        :return: list of folders
        """
        prefix = "{}/{}/".format(self.aws_account_id, self.aws_region)
        try:
            result = self.s3_client.list_objects_v2(
                Bucket=self.access_log_bucket, Prefix=prefix, Delimiter="/"
            )
            if "CommonPrefixes" in result:
                folders = [prefix["Prefix"] for prefix in result["CommonPrefixes"]]
                return folders
            return []
        except Exception as e:
            logging.warning(f"Error listing S3 folders: {e}")
            return []

    def run(self) -> None:
        """
        Execute compaction job for a single date or backfill
        """
        spark = _start_spark()
        buckets = self.get_list_of_folders()
        logging.info(f"Found {len(buckets)} buckets to process")

        if self.start_date:
            # If doing backfill iterate each day starting from start_date until the normal lookback_day
            start_date = parse(self.start_date)
            current_date = start_date
            while current_date <= self.lookback_days:
                logging.info(f"Processing data for {current_date.strftime('%Y/%m/%d')}")
                self.compact(buckets=buckets, run_date=current_date, spark=spark)
                current_date += timedelta(days=1)
        else:
            logging.info(f"Processing data for {self.lookback_days.strftime('%Y/%m/%d')}")
            self.compact(buckets=buckets, run_date=self.lookback_days, spark=spark)

        try:
            spark.stop()
        except Exception as e:
            logging.warning(f"Error when stopping spark: {e}")

    def compact(self, buckets: List[Any], run_date, spark) -> None:
        """
        Read, parse, and compact S3 server access logs using PySpark and EMR
        :param buckets: List of buckets/folders to compact
        :param run_date: Date string to run on
        :param spark: Spark client
        """
        for bucket in buckets:
            start_time = time.time()
            logging.info(f"Processing bucket {bucket}")

            s3_logs_paths = get_s3a_paths(
                self.s3_client,
                self.access_log_bucket,
                "{}{}".format(bucket, run_date.strftime("%Y/%m/%d")),
            )
            if len(s3_logs_paths) > 0:
                s3_path_rdd = spark.sparkContext.parallelize(
                    s3_logs_paths, numSlices=self.num_parallelize
                )
                contents_rdd = s3_path_rdd.flatMap(
                    lambda s3_path: s3_read_file(_get_aws_creds(), s3_path)
                )
                access_logs_df = spark.createDataFrame(contents_rdd, S3_ACCESS_LOG_OUTPUT_SCHEMA)
                spark.sparkContext.setJobDescription("s3_access_log_rollup")

                # TODO consider moving some of this to functions for easier testing
                access_logs_df = access_logs_df.withColumn(
                    "request_time",
                    to_timestamp(
                        access_logs_df.request_time_string,
                        format="yyyy-MM-dd'T'HH:mm:ss",
                    ),
                )
                # can be adjusted to remove unwanted columns
                access_logs_df = access_logs_df.select(
                    "bucket_owner",
                    "s3_bucket",
                    "request_time",
                    "remote_ip",
                    "requester",
                    "request_id",
                    "operation",
                    "key",
                    "request",
                    "http_status",
                    "error_code",
                    "bytes_sent",
                    "object_size",
                    "total_time",
                    "turn_around_time",
                    "referrer",
                    "user_agent",
                    "version_id",
                    "host_id",
                    "signature_version",
                    "cipher_suite",
                    "authentication_type",
                    "host_header",
                    "tls_version",
                    "access_point_arn",
                    "acl_required",
                    "error_line",
                )
                sort_keys = ["request_time"]
                partition_columns_names = []

                if self.hive_formatted_folders:
                    date_format = "year=%Y/month=%m/day=%d"
                else:
                    date_format = "%Y/%m/%d"
                destination = "s3a://{logs_bucket}/{prefix}/{source_bucket}{date}".format(
                    logs_bucket=self.destination_log_bucket,
                    prefix=self.destination_log_prefix,
                    source_bucket=bucket,
                    date=run_date.strftime(date_format),
                )

                # repartition into specific number of files and sort
                access_logs_df.repartition(self.num_partitions).sortWithinPartitions(
                    sort_keys
                ).write.mode("overwrite").option("compression", "snappy").partitionBy(
                    partition_columns_names
                ).parquet(
                    destination
                )
                end_time = time.time()
                elapsed_time = end_time - start_time
                logging.info(f"Processing bucket {bucket} completed in {elapsed_time} seconds")


def parse_arguments() -> dict:
    """
    Parse command line arguments and set common sense defaults
    :return: args dict
    """
    params = argparse.ArgumentParser()

    params.add_argument(
        "--aws-account-id",
        default=0,
        type=int,
        help="The AWS Account ID of the logs. Will use caller identity if not set",
    )
    params.add_argument(
        "--aws-region",
        default="us-east-1",
        help="The AWS region of the logs. Default: %(default)s",
    )
    params.add_argument(
        "--lookback-days",
        default=1,
        type=int,
        help="Number of days in the past to run job on Default: %(default)s",
    )
    params.add_argument(
        "--access-log-bucket",
        help="The S3 bucket containing S3 server side logging files.",
    )
    params.add_argument(
        "--destination-log-bucket",
        default="same",
        help="The S3 bucket storing compacted S3 server side logging files. Default: same bucket",
    )
    params.add_argument(
        "--destination-log-prefix",
        default="processed",
        help="S3 prefix used to store processed log files. Default: %(default)s",
    )
    params.add_argument(
        "--num-output-files",
        default=10,
        type=int,
        help="Number of output files. Default: %(default)s",
    )
    params.add_argument(
        "--hive-formatted-folders",
        type=bool,
        default=False,
        help="Structure output folders in Hive format for easier query with Athena. Default: %(default)s",
    )
    params.add_argument(
        "--num-parallelize",
        default=100,
        type=int,
        help="Number of parallelize jobs to split into. Default: %(default)s",
    )
    params.add_argument(
        "--start-date",
        default=False,
        help="Start date of backfill, if set will run in loop until caught up",
    )

    args = params.parse_args()
    arg_dict = {key: value for key, value in vars(args).items() if value is not None}

    return arg_dict


if __name__ == "__main__":
    kwargs = parse_arguments()
    job = S3ServerSideLoggingRollup(**kwargs)
    job.run()
