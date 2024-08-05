-- This creates a Athena table with partition projection enabled
CREATE EXTERNAL TABLE `your-s3-bucket-log-name`(
  `bucket_owner` string COMMENT '',
  `s3_bucket` string COMMENT '',
  `request_time` timestamp COMMENT '',
  `remote_ip` string COMMENT '',
  `requester` string COMMENT '',
  `request_id` string COMMENT '',
  `operation` string COMMENT '',
  `key` string COMMENT '',
  `request` string COMMENT '',
  `http_status` int COMMENT '',
  `error_code` string COMMENT '',
  `bytes_sent` bigint COMMENT '',
  `object_size` bigint COMMENT '',
  `total_time` bigint COMMENT '',
  `turn_around_time` bigint COMMENT '',
  `referrer` string COMMENT '',
  `user_agent` string COMMENT '',
  `version_id` string COMMENT '',
  `host_id` string COMMENT '',
  `signature_version` string COMMENT '',
  `cipher_suite` string COMMENT '',
  `authentication_type` string COMMENT '',
  `host_header` string COMMENT '',
  `tls_version` string COMMENT '',
  `access_point_arn` string COMMENT '',
  `acl_required` string COMMENT '',
  `error_line` string COMMENT '')
PARTITIONED BY (
    date string
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://your-dest-bucket/processed/your-account-id/us-east-1/your-s3-bucket-log-name'
TBLPROPERTIES (
  "projection.enabled" = "true",
  "projection.date.type" = "date",
  "projection.date.range" = "2020/01/01,NOW",
  "projection.date.format" = "yyyy/MM/dd",
  "storage.location.template" = "s3://your-dest-bucket/processed/your-account-id/us-east-1/your-s3-bucket-log-name/${date}/"
)
