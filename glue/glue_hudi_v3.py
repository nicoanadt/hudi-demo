import sys
import os
import json

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import concat, col, lit, to_timestamp

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

import boto3
from botocore.exceptions import ClientError



args = getResolvedOptions(sys.argv, ['JOB_NAME'])

spark = SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer').getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger = glueContext.get_logger()

logger.info('Initialization.')
glueClient = boto3.client('glue')
ssmClient = boto3.client('ssm')
redshiftDataClient = boto3.client('redshift-data')

logger.info('Fetching configuration.')
region = os.environ['AWS_DEFAULT_REGION']

###
curatedS3BucketName = 'hudiglueblog-curateds3bucket-16xp57k12ie0q'
rawS3BucketName = 'hudiglueblog-raws3bucket-peeeobltsx6i'
hudiStorageType = 'CoW'

###
dropColumnList = ['db', 'table_name']
dropColumnList_2 = ['db', 'table_name', 'Op']

dbName = 'human_resources'
tableName = 'employee_details'
tableNameTgt = 'employee_details_2'
isPartitionKey = True
partitionKey = 'department'
primaryKey = 'emp_no'
isTableExists = True

###

sourcePath = 's3://' + rawS3BucketName + '/' + dbName + '/' + tableName
targetPath = 's3://' + curatedS3BucketName + '/' + dbName + '/' + tableNameTgt

morConfig = {'hoodie.datasource.write.storage.type': 'MERGE_ON_READ', 'hoodie.compact.inline': 'false',
             'hoodie.compact.inline.max.delta.commits': 20, 'hoodie.parquet.small.file.limit': 0}

commonConfig = {'className': 'org.apache.hudi',
                'hoodie.datasource.hive_sync.use_jdbc': 'false',
                'hoodie.datasource.write.precombine.field': 'update_ts_dms',
                'hoodie.datasource.write.recordkey.field': primaryKey,
                'hoodie.table.name': tableNameTgt,
                'hoodie.consistency.check.enabled': 'true',
                'hoodie.datasource.hive_sync.database': dbName,
                'hoodie.datasource.hive_sync.table': tableNameTgt,
                'hoodie.datasource.hive_sync.enable': 'true',
                'path': targetPath}

partitionDataConfig = {'hoodie.datasource.write.partitionpath.field': partitionKey,
                       'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
                       'hoodie.datasource.hive_sync.partition_fields': partitionKey}

unpartitionDataConfig = {
    'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.NonPartitionedExtractor',
    'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.NonpartitionedKeyGenerator'}


incrementalConfig = {'hoodie.upsert.shuffle.parallelism': 20, 'hoodie.datasource.write.operation': 'upsert',
                     'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS', 'hoodie.cleaner.commits.retained': 10,
                    'hoodie.datasource.write.payload.class': 'org.apache.hudi.payload.AWSDmsAvroPayload'}




initLoadConfig = {'hoodie.bulkinsert.shuffle.parallelism': 3,
                  'hoodie.datasource.write.operation': 'bulk_insert'}



###

inputDyf = glueContext.create_dynamic_frame_from_options(connection_type='s3', connection_options={
    'paths': [sourcePath], 'groupFiles': 'none',
    'recurse': True}, format='parquet', transformation_ctx=tableName)

inputDf = inputDyf.toDF().withColumn('update_ts_dms', to_timestamp(col('update_ts_dms')))


### UPSERT ROWS

logger.info('Incremental load.')
outputDf = inputDf.drop(*dropColumnList)


if (isTableExists):

    if outputDf.count() > 0:
        logger.info('Upserting data.')
        if (isPartitionKey):
            logger.info('Writing to partitioned Hudi table.')
            outputDf = outputDf.withColumn(partitionKey, concat(lit(partitionKey + '='), col(partitionKey)))
            combinedConf = {**commonConfig, **partitionDataConfig, **incrementalConfig}
            #outputDf.write.format('org.apache.hudi').options(**combinedConf).mode('Append').save(targetPath)
            glueContext.write_dynamic_frame.from_options(
                frame=DynamicFrame.fromDF(outputDf, glueContext, "outputDf"),
                connection_type="marketplace.spark", connection_options=combinedConf)
        else:
            logger.info('Writing to unpartitioned Hudi table.')
            combinedConf = {**commonConfig, **unpartitionDataConfig, **incrementalConfig}
            #outputDf.write.format('org.apache.hudi').options(**combinedConf).mode('Append').save(targetPath)
            glueContext.write_dynamic_frame.from_options(
                frame=DynamicFrame.fromDF(outputDf, glueContext, "outputDf"),
                connection_type="marketplace.spark", connection_options=combinedConf)

### B. NEW TABLE (Initial Load)

else:
    outputDf = inputDf.drop(*dropColumnList_2)
    if outputDf.count() > 0:
        logger.info('Inital load.')
        if (isPartitionKey):
            logger.info('Writing to partitioned Hudi table.')
            outputDf = outputDf.withColumn(partitionKey, concat(lit(partitionKey + '='), col(partitionKey)))
            combinedConf = {**commonConfig, **partitionDataConfig, **initLoadConfig}
            #outputDf.write.format('org.apache.hudi').options(**combinedConf).mode('Overwrite').save(targetPath)
            glueContext.write_dynamic_frame.from_options(
                frame = DynamicFrame.fromDF(outputDf, glueContext, "outputDf"),
                connection_type = "marketplace.spark", connection_options = combinedConf)
        else:
            logger.info('Writing to unpartitioned Hudi table.')
            combinedConf = {**commonConfig, **unpartitionDataConfig, **initLoadConfig}
            #outputDf.write.format('org.apache.hudi').options(**combinedConf).mode('Overwrite').save(targetPath)
            glueContext.write_dynamic_frame.from_options(
                frame = DynamicFrame.fromDF(outputDf, glueContext, "outputDf"),
                connection_type = "marketplace.spark", connection_options = combinedConf)
                
                
                
