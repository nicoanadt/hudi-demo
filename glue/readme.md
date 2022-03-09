## Sample glue job that demonstrate Apache Hudi deployment in AWS Glue.

1. This is based on the sample provided by [this blog](https://aws.amazon.com/blogs/big-data/creating-a-source-to-lakehouse-data-replication-pipe-using-apache-hudi-aws-glue-aws-dms-and-amazon-redshift/) and enriched with Hudi connector for glue as described [here](https://aws.amazon.com/blogs/big-data/writing-to-apache-hudi-tables-using-aws-glue-connector/)
2. Job details information: 
  - Add Apache Hudi Connector from AWS Marketplace, include as connection in glue job
  - Add spark-avro_2.11-2.4.4.jar in dependent JARs path
  - Enable job bookmark
  - Job parameters: `--additional-python-modules` : `botocore>=1.20.12,boto3>=1.17.12`


