<h1 align="center">AWS Logs Parquet Converter</h1>
<p align="center">
  <img src="https://img.shields.io/badge/license-MIT-green">
</p>

## Overview
This repository contains a collection of PySpark scripts designed to convert AWS logs (VPC, S3, ALB) into a more valuable and storage-dense format. Many organizations use AWS logs to audit and understand their platform's usage but often face challenges with the cost and scale of the data generated. These scripts help parse, compress, and reformat the data to reduce both raw storage costs and query costs when using tools like AWS Athena.

## Why Use This Project
- **Cost Savings:** Reduce raw storage costs by compressing logs using Parquet format.
- **Query Optimization:** Improve query performance and reduce costs in AWS Athena by using optimized data formats.
- **Ease of Use:** Ready-to-use PySpark scripts that integrate seamlessly into your existing workflows.

## Technologies
- **PySpark:** For efficient data processing.
- **Parquet:** For compressed, columnar data storage.

## Usage
For more information on how to use and deploy these scripts review the README in the converter folders.

## Contributing
We welcome contributions! Please see our [contributing](CONTRIBUTING.md) guidelines for more details.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contact
For any questions or support, please open an issue or create a PR!

## Final Note
There are many different ways to tackle these problems.  At PebblePost we decided to turn to PySpark and Parquet as they are common place in our environment.  We hope that by sharing these scripts we can save other DevOps team time and money. 
