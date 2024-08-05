# S3 Server Access Logs Converter

AWS S3 Server Side Logging or Server Access Logs are a common way to track what actions occur in an S3 bucket for security or auditing purposes.  However, the way AWS generates these logs can be problematic.  These logs are generated in a raw format that is space inefficient and with a seemingly random splay of file sizes and counts.  It is common to see well over 100k files with sizes ranging from bytes to several hundred kilobytes only for a single reporting date.

This leads to issues when trying to apply lifecycle rules such as [intelligent-tiering](https://aws.amazon.com/s3/storage-classes/intelligent-tiering/) or just querying with [Athena](https://stackoverflow.com/questions/57516526/athena-performance-on-too-many-s3-files).

To overcome these issues, this script parses and converts the raw log format into a columnar format.  The data for each day is parsed and re-partitioned and then outputs into Snappy-compressed Parquet files.  The average daily folder size in our testing dropped by ~80%.  However, this may vary wildly depending on your S3 usage and how much duplication is contained in the logs.  Because we are also sorting and re-partitioning the data, the number of files also drops to a configurable amount, which helps with Athena queries.

## Dependencies
- Only compatible with AWS S3 Server Side Logging
- Only compatible with data-based partitioning
- Tested with Python 3.10 and Spark 3.5
- Tested with AWS EMR Serverless, should work with any Spark-compatible system.

## How it works

The script is written with the assumption it will be run daily.  If using the included SAM deployment, this is handled via an AWS EventBridge rule and Step Function script.  The script will then perform the following functions:
1. Start a Spark cluster
2. Look for all folders under `/{aws_account_id}/{aws_region}/`
   1. Both of these values can be configured via args
3. For each folder(bucket) pull the data from the day to be processed
   1. By default, this processes the previous day
   2. Assuming path of `/{aws_account_id}/{aws_region}/{folder}/{year}/{month}/{day}`
4. Submit the spark job to compact the data at that folder/date path
5. Repeat until all folders(buckets) completed
6. Stop Spark cluster


## How to deploy - SAM
This deploy assumes a pre-existing EMR Serverless cluster and IAM roles.

1. Upload `s3_server_access_logs.py` to S3 bucket accessible from EMR
2. Navigate to the `/deploy` folder
3. Run `sam deploy -g`
4. Fill-in or use default options
5. Wait for CloudFormation to complete


## Credit

This script is heavily based on the Yelp repo [aws_logs_to_parquet_converter](https://github.com/Yelp/aws_logs_to_parquet_converter/tree/master).
