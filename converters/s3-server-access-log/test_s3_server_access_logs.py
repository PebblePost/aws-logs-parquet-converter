import os
import unittest
import pytest
import boto3

from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
from moto import mock_aws
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession

from s3_server_access_logs import (
    _get_aws_creds,
    _start_spark,
    get_s3a_paths,
    list_bucket_with_prefix,
    s3_read_file,
    parse_s3_access_time,
    S3ServerSideLoggingRollup,
    parse_arguments,
)


class TestNonClassFunctions(unittest.TestCase):
    def setUp(self):
        # force ContinuationToken when more than 10 keys
        os.environ["MOTO_S3_DEFAULT_MAX_KEYS"] = "10"

        self.mock_aws = mock_aws()
        self.mock_aws.start()

        # Set up mock S3 environment
        self.s3_client = boto3.client("s3", region_name="us-east-1")
        self.bucket = "test-bucket"
        self.prefix = "test-prefix/"

        # Create bucket and upload test objects
        self.s3_client.create_bucket(Bucket=self.bucket)
        self.s3_client.put_object(
            Bucket=self.bucket, Key=f"{self.prefix}file1.txt", Body=b"content"
        )
        self.s3_client.put_object(
            Bucket=self.bucket, Key=f"{self.prefix}file2.txt", Body=b"content"
        )
        self.s3_client.put_object(Bucket=self.bucket, Key="other-prefix/file3.txt", Body=b"content")

    def tearDown(self) -> None:
        self.mock_aws.stop()

    @patch.dict(
        os.environ,
        {
            "AWS_ACCESS_KEY_ID": "test_access_key",
            "AWS_SECRET_ACCESS_KEY": "test_secret_key",
            "AWS_DEFAULT_REGION": "us-west-2",
        },
    )
    def test_get_aws_creds_with_all_env_vars(self):
        expected_creds = {
            "aws_access_key_id": "test_access_key",
            "aws_secret_access_key": "test_secret_key",
            "region_name": "us-west-2",
        }
        self.assertEqual(_get_aws_creds(), expected_creds)

    @patch.dict(
        os.environ,
        {"AWS_ACCESS_KEY_ID": "test_access_key", "AWS_SECRET_ACCESS_KEY": "test_secret_key"},
    )
    def test_get_aws_creds_with_default_region(self):
        expected_creds = {
            "aws_access_key_id": "test_access_key",
            "aws_secret_access_key": "test_secret_key",
            "region_name": "us-east-1",  # Default region
        }
        self.assertEqual(_get_aws_creds(), expected_creds)

    @patch.dict(os.environ, {}, clear=True)
    def test_get_aws_creds_with_no_env_vars(self):
        expected_creds = {
            "aws_access_key_id": None,
            "aws_secret_access_key": None,
            "region_name": "us-east-1",  # Default region
        }
        self.assertEqual(_get_aws_creds(), expected_creds)

    @pytest.mark.skip(reason="works but actually starts a session, remove to test changes")
    def test_start_spark(self):

        spark = None
        try:
            spark = _start_spark()
            self.assertIsInstance(spark, SparkSession)

            # Check Spark configuration
            self.assertEqual(spark.conf.get("spark.app.name"), "s3_server_side_log_compacter")
            self.assertEqual(spark.conf.get("spark.speculation"), "false")
            self.assertEqual(
                spark.conf.get("spark.sql.parquet.outputTimestampType"), "TIMESTAMP_MILLIS"
            )
            self.assertEqual(
                spark.conf.get("spark.jars.packages"), "org.apache.hadoop:hadoop-aws:3.3.1"
            )

            # Check Hadoop configuration
            hadoop_conf = spark._jsc.hadoopConfiguration()
            self.assertEqual(
                hadoop_conf.get("mapreduce.fileoutputcommitter.algorithm.version"), "2"
            )

        finally:
            if spark:
                spark.stop()

    def test_get_s3a_paths(self):
        # Test function to get s3a paths
        expected_paths = [
            f"s3a://{self.bucket}/{self.prefix}file1.txt",
            f"s3a://{self.bucket}/{self.prefix}file2.txt",
        ]
        result = get_s3a_paths(self.s3_client, self.bucket, self.prefix)
        self.assertEqual(sorted(result), sorted(expected_paths))

    def test_list_bucket_with_prefix(self):
        # Test function to list bucket contents with prefix
        expected_keys = [f"{self.prefix}file1.txt", f"{self.prefix}file2.txt"]
        result = list_bucket_with_prefix(self.s3_client, self.bucket, self.prefix)
        self.assertEqual(sorted(result), sorted(expected_keys))

    def test_list_bucket_with_prefix_no_keys(self):
        # Test function when no keys match the prefix
        result = list_bucket_with_prefix(self.s3_client, self.bucket, "non-existent-prefix/")
        self.assertEqual(result, [])

    def test_list_bucket_with_prefix_pagination(self):
        # Test function with pagination
        for i in range(50):
            self.s3_client.put_object(
                Bucket=self.bucket, Key=f"{self.prefix}file{i}.txt", Body=b"content"
            )

        result = list_bucket_with_prefix(self.s3_client, self.bucket, self.prefix)
        expected_keys = [f"{self.prefix}file{i}.txt" for i in range(50)]
        self.assertEqual(sorted(result), sorted(expected_keys))

    @patch("s3_server_access_logs.boto3.client")
    def test_list_bucket_with_client_error(self, mock_boto_client):
        mock_s3_client = mock_boto_client.return_value
        mock_s3_client.list_objects_v2.side_effect = ClientError(
            {"Error": {"Code": "NoSuchBucket", "Message": "The specified bucket does not exist."}},
            "ListObjectsV2",
        )

        result = list_bucket_with_prefix(mock_s3_client, self.bucket, self.prefix)
        self.assertEqual(result, [])  # Expect an empty list due to the error

    @mock_aws()
    def test_s3_read_file(self):
        # Set up mock S3 environment
        s3_client = boto3.client("s3", region_name="us-east-1")
        bucket = "test-bucket"
        prefix = "test-prefix"

        # Create bucket and upload test objects
        s3_client.create_bucket(Bucket=bucket)
        s3_client.put_object(Bucket=bucket, Key=f"{prefix}/file1.txt", Body=b"content\ncontent2")
        aws_cred_dict = {
            "aws_access_key_id": "fake_key",
            "aws_secret_access_key": "fake_secret",
            "region_name": "us-east-1",
        }
        path = f"s3://{bucket}/{prefix}/file1.txt"
        res = s3_read_file(aws_cred_dict, path)
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0].error_line, "content")
        self.assertEqual(res[0].s3_bucket, None)

    def test_s3_read_file_real_file(self):
        # Set up mock S3 environment
        s3_client = boto3.client("s3", region_name="us-east-1")
        bucket = "test-bucket"
        prefix = "test-prefix"

        # Create bucket and upload test objects
        s3_client.create_bucket(Bucket=bucket)
        s3_client.put_object(
            Bucket=bucket,
            Key=f"{prefix}/file1.txt",
            Body=b'2f72d68abcde93a1d8fd610f6b970dc1cabf1ddc49890eff97a5f24db329007e test-bucket [30/Jul/2024:13:08:44 +0000] 1.1.1.1 arn:aws:sts::123456789:assumed-role/TestRole/i-123456789abcdefgh BZCH9YMEC2WD3E4Z REST.HEAD.BUCKET - "HEAD / HTTP/1.1" 200 - - - 26 26 "-" "Hadoop 3.3.1, aws-sdk-java/1.12.189 Linux/5.15.0-1064-aws OpenJDK_64-Bit_Server_VM/25.382-b05 java/1.8.0_382 scala/2.12.14 vendor/Azul_Systems,_Inc. cfg/retry-mode/legacy" - ywUV+WX4hdG+LOGtBznXDFARyoT1wwLmHy9FrxRqfz25a86QIizV6v8bYK9/FCDpRYBzknx4/e4= SigV4 TLS_AES_128_GCM_SHA256 AuthHeader test-bucket.s3.amazonaws.com TLSv1.3 - -',
        )

        aws_cred_dict = {
            "aws_access_key_id": "fake_key",
            "aws_secret_access_key": "fake_secret",
            "region_name": "us-east-1",
        }
        path = f"s3://{bucket}/{prefix}/file1.txt"
        res = s3_read_file(aws_cred_dict, path)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0].error_line, None)
        self.assertEqual(res[0].s3_bucket, "test-bucket")

    def test_parse_s3_access_time(self):
        # Example timestamp string with a valid format
        input_str = "[30/Jul/2024:13:08:44 +0000] Some event details"
        expected = datetime(2024, 7, 30, 13, 8, 44)
        result = parse_s3_access_time(input_str)
        self.assertEqual(result, expected)

    def test_parse_s3_access_time_invalid_format(self):
        # Timestamp string with an invalid format
        input_str = "[InvalidTimestamp] Some event details"
        result = parse_s3_access_time(input_str)
        self.assertIsNone(result)

    @patch("sys.argv", ["script_name", "--access-log-bucket", "my-access-log-bucket"])
    def test_parse_arguments_defaults(self):
        # Test with no arguments, should use all defaults
        expected = {
            "aws_account_id": 0,
            "aws_region": "us-east-1",
            "lookback_days": 1,
            "access_log_bucket": "my-access-log-bucket",
            "destination_log_bucket": "same",
            "destination_log_prefix": "processed",
            "num_output_files": 10,
            "hive_formatted_folders": False,
            "num_parallelize": 100,
            "start_date": False,
        }
        args = parse_arguments()
        self.assertEqual(args, expected)

    @patch(
        "sys.argv",
        [
            "script_name",
            "--aws-account-id",
            "123456789012",
            "--aws-region",
            "us-west-2",
            "--lookback-days",
            "5",
            "--access-log-bucket",
            "my-access-log-bucket",
            "--destination-log-bucket",
            "my-destination-log-bucket",
            "--destination-log-prefix",
            "my-prefix",
            "--num-output-files",
            "20",
            "--hive-formatted-folders",
            "True",
            "--num-parallelize",
            "200",
            "--start-date",
            "2024-01-01",
        ],
    )
    def test_parse_arguments_with_arguments(self):
        # Test with all arguments provided
        expected = {
            "aws_account_id": 123456789012,
            "aws_region": "us-west-2",
            "lookback_days": 5,
            "access_log_bucket": "my-access-log-bucket",
            "destination_log_bucket": "my-destination-log-bucket",
            "destination_log_prefix": "my-prefix",
            "num_output_files": 20,
            "hive_formatted_folders": True,
            "num_parallelize": 200,
            "start_date": "2024-01-01",
        }
        args = parse_arguments()
        self.assertEqual(args, expected)


class TestS3ServerSideLoggingRollup(unittest.TestCase):
    @patch("boto3.client")
    def test_init(self, mock_boto_client):
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        rollup = S3ServerSideLoggingRollup(
            aws_account_id=123456789012,
            lookback_days=7,
            aws_region="us-east-1",
            access_log_bucket="access-log-bucket",
            destination_log_bucket="destination-log-bucket",
            destination_log_prefix="prefix",
            num_output_files=10,
            hive_formatted_folders=True,
            num_parallelize=4,
            start_date=None,
        )

        self.assertEqual(rollup.s3_client, mock_client)
        self.assertEqual(rollup.aws_account_id, 123456789012)
        self.assertEqual(rollup.lookback_days.day, (datetime.today() - timedelta(days=7)).day)
        self.assertEqual(rollup.destination_log_bucket, "destination-log-bucket")
        mock_boto_client.assert_called_once_with("s3", **_get_aws_creds())

    @patch("boto3.client")
    def test_init_same_dest(self, mock_boto_client):
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        rollup = S3ServerSideLoggingRollup(
            aws_account_id=123456789012,
            lookback_days=7,
            aws_region="us-east-1",
            access_log_bucket="access-log-bucket",
            destination_log_bucket="same",
            destination_log_prefix="prefix",
            num_output_files=10,
            hive_formatted_folders=True,
            num_parallelize=4,
            start_date=None,
        )

        self.assertEqual(rollup.s3_client, mock_client)
        self.assertEqual(rollup.aws_account_id, 123456789012)
        self.assertEqual(rollup.lookback_days.day, (datetime.today() - timedelta(days=7)).day)
        self.assertEqual(rollup.destination_log_bucket, "access-log-bucket")
        mock_boto_client.assert_called_once_with("s3", **_get_aws_creds())

    @patch("boto3.client")
    def test_init_connect_to_s3_failure(self, mock_boto_client):
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client
        mock_boto_client.side_effect = Exception("S3 connection failed")

        with self.assertRaises(Exception) as context:
            rollup = S3ServerSideLoggingRollup(
                aws_account_id=123456789012,
                lookback_days=7,
                aws_region="us-east-1",
                access_log_bucket="access-log-bucket",
                destination_log_bucket="destination-log-bucket",
                destination_log_prefix="prefix",
                num_output_files=10,
                hive_formatted_folders=True,
                num_parallelize=4,
                start_date=None,
            )
        self.assertEqual(str(context.exception), "S3 connection failed")
        mock_boto_client.assert_called_once_with("s3", **_get_aws_creds())

    @patch("boto3.client")
    def test_init_get_aws_account_id_sts(self, mock_boto_client):
        mock_sts_client = MagicMock()
        mock_boto_client.return_value = mock_sts_client
        mock_sts_client.get_caller_identity.return_value = {"Account": "987654321098"}

        rollup = S3ServerSideLoggingRollup(
            aws_account_id=0,  # Use STS to get the account ID
            lookback_days=7,
            aws_region="us-east-1",
            access_log_bucket="access-log-bucket",
            destination_log_bucket="destination-log-bucket",
            destination_log_prefix="prefix",
            num_output_files=10,
            hive_formatted_folders=True,
            num_parallelize=4,
            start_date=None,
        )

        self.assertEqual(rollup.aws_account_id, "987654321098")
        mock_boto_client.assert_any_call("sts", **_get_aws_creds())
        mock_boto_client.assert_any_call("s3", **_get_aws_creds())
        mock_sts_client.get_caller_identity.assert_called_once()

    @patch("boto3.client")
    def test_init_get_aws_account_id_sts_failure(self, mock_boto_client):
        mock_sts_client = MagicMock()
        mock_sts_client.get_caller_identity.side_effect = Exception("STS call failed")
        mock_boto_client.return_value = mock_sts_client

        with self.assertRaises(Exception) as context:
            rollup = S3ServerSideLoggingRollup(
                aws_account_id=0,  # Use STS to get the account ID
                lookback_days=7,
                aws_region="us-east-1",
                access_log_bucket="access-log-bucket",
                destination_log_bucket="destination-log-bucket",
                destination_log_prefix="prefix",
                num_output_files=10,
                hive_formatted_folders=True,
                num_parallelize=4,
                start_date=None,
            )
            self.assertEqual(rollup.aws_account_id, "0")

        self.assertEqual(str(context.exception), "STS call failed")
        mock_boto_client.assert_any_call("sts", **_get_aws_creds())
        mock_boto_client.assert_any_call("s3", **_get_aws_creds())
        mock_sts_client.get_caller_identity.assert_called_once()

    @patch("boto3.client")
    def test_get_list_of_folders_success(self, mock_boto_client):
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client
        mock_s3_client.list_objects_v2.return_value = {
            "CommonPrefixes": [
                {"Prefix": "123456789012/us-east-1/folder1/"},
                {"Prefix": "123456789012/us-east-1/folder2/"},
            ]
        }

        rollup = S3ServerSideLoggingRollup(
            aws_account_id=123456789012,
            lookback_days=7,
            aws_region="us-east-1",
            access_log_bucket="access-log-bucket",
            destination_log_bucket="destination-log-bucket",
            destination_log_prefix="prefix",
            num_output_files=10,
            hive_formatted_folders=True,
            num_parallelize=4,
            start_date=None,
        )

        folders = rollup.get_list_of_folders()

        self.assertEqual(
            folders, ["123456789012/us-east-1/folder1/", "123456789012/us-east-1/folder2/"]
        )
        mock_s3_client.list_objects_v2.assert_called_once_with(
            Bucket="access-log-bucket", Prefix="123456789012/us-east-1/", Delimiter="/"
        )

    @patch("boto3.client")
    def test_get_list_of_folders_no_folders(self, mock_boto_client):
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client
        mock_s3_client.list_objects_v2.return_value = {"CommonPrefixes": []}

        rollup = S3ServerSideLoggingRollup(
            aws_account_id=123456789012,
            lookback_days=7,
            aws_region="us-east-1",
            access_log_bucket="access-log-bucket",
            destination_log_bucket="destination-log-bucket",
            destination_log_prefix="prefix",
            num_output_files=10,
            hive_formatted_folders=True,
            num_parallelize=4,
            start_date=None,
        )

        folders = rollup.get_list_of_folders()

        self.assertEqual(folders, [])
        mock_s3_client.list_objects_v2.assert_called_once_with(
            Bucket="access-log-bucket", Prefix="123456789012/us-east-1/", Delimiter="/"
        )

    @patch("boto3.client")
    def test_get_list_of_folders_s3_failure(self, mock_boto_client):
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client
        mock_s3_client.list_objects_v2.side_effect = Exception("S3 call failed")
        mock_s3_client.list_objects_v2.return_value = {
            "CommonPrefixes": [
                {"Prefix": "123456789012/us-east-1/folder1/"},
                {"Prefix": "123456789012/us-east-1/folder2/"},
            ]
        }

        rollup = S3ServerSideLoggingRollup(
            aws_account_id=123456789012,
            lookback_days=7,
            aws_region="us-east-1",
            access_log_bucket="access-log-bucket",
            destination_log_bucket="destination-log-bucket",
            destination_log_prefix="prefix",
            num_output_files=10,
            hive_formatted_folders=True,
            num_parallelize=4,
            start_date=None,
        )

        folders = rollup.get_list_of_folders()
        self.assertEqual(folders, [])
        mock_s3_client.list_objects_v2.assert_called_once_with(
            Bucket="access-log-bucket", Prefix="123456789012/us-east-1/", Delimiter="/"
        )

    @patch("s3_server_access_logs._start_spark")
    @patch("s3_server_access_logs.S3ServerSideLoggingRollup.get_list_of_folders")
    @patch("s3_server_access_logs.S3ServerSideLoggingRollup.compact")
    def test_run_backfill(self, mock_compact, mock_get_list_of_folders, mock_start_spark):
        mock_spark = MagicMock()
        mock_start_spark.return_value = mock_spark
        mock_get_list_of_folders.return_value = ["123456789012/us-east-1/folder1/"]
        start_date = (datetime.today() - timedelta(days=3)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        rollup = S3ServerSideLoggingRollup(
            aws_account_id=123456789012,
            lookback_days=1,
            aws_region="us-east-1",
            access_log_bucket="access-log-bucket",
            destination_log_bucket="destination-log-bucket",
            destination_log_prefix="prefix",
            num_output_files=10,
            hive_formatted_folders=True,
            num_parallelize=4,
            start_date=start_date.strftime("%Y-%m-%d"),
        )

        rollup.run()

        for i in range(3):
            mock_compact.assert_any_call(
                buckets=["123456789012/us-east-1/folder1/"],
                run_date=start_date + timedelta(days=i),
                spark=mock_spark,
            )
        mock_start_spark.assert_called_once()
        mock_spark.stop.assert_called_once()

    @patch("s3_server_access_logs._start_spark")
    @patch("s3_server_access_logs.S3ServerSideLoggingRollup.get_list_of_folders")
    @patch("s3_server_access_logs.S3ServerSideLoggingRollup.compact")
    def test_run_single_date(self, mock_compact, mock_get_list_of_folders, mock_start_spark):
        mock_spark = MagicMock()
        mock_start_spark.return_value = mock_spark
        mock_get_list_of_folders.return_value = ["123456789012/us-east-1/folder1/"]
        start_date = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

        rollup = S3ServerSideLoggingRollup(
            aws_account_id=123456789012,
            lookback_days=1,
            aws_region="us-east-1",
            access_log_bucket="access-log-bucket",
            destination_log_bucket="destination-log-bucket",
            destination_log_prefix="prefix",
            num_output_files=10,
            hive_formatted_folders=True,
            num_parallelize=4,
            start_date=None,
        )

        rollup.run()

        mock_compact.assert_called_once()
        mock_start_spark.assert_called_once()
        mock_spark.stop.assert_called_once()


# TODO get tests for compact working
# @patch("boto3.client")
# @patch('s3_server_access_logs.get_s3a_paths')
# @patch('s3_server_access_logs.s3_read_file')
# def test_compact(self, mock_s3_read_file, mock_get_s3a_paths, mock_boto_client):
#     # Arrange
#     mock_spark = MagicMock()
#
#     mock_get_s3a_paths.return_value = ['s3a://bucket/path1', 's3a://bucket/path2']
#     mock_s3_read_file.return_value = [{'key': 'value'}]
#
#     rollup = S3ServerSideLoggingRollup(
#         aws_account_id=123456789012,
#         lookback_days=1,
#         aws_region='us-east-1',
#         access_log_bucket='access-log-bucket',
#         destination_log_bucket='destination-log-bucket',
#         destination_log_prefix='prefix',
#         num_output_files=10,
#         hive_formatted_folders=True,
#         num_parallelize=4,
#         start_date=None
#     )
#
#     run_date = datetime(2024, 7, 30, 0, 0, 0)
#     buckets = ['123456789012/us-east-1/folder1/']
#
#     rollup.compact(buckets=buckets, run_date=run_date, spark=mock_spark)
#
#     mock_get_s3a_paths.assert_called_once_with(
#         rollup.s3_client,
#         rollup.access_log_bucket,
#         '123456789012/us-east-1/folder1/2024/07/30'
#     )
