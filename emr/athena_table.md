## Create Table Command

This is create table command sample for Athena

```
CREATE EXTERNAL TABLE `employee_details`(
  `_hoodie_commit_time` string, 
  `_hoodie_commit_seqno` string, 
  `_hoodie_record_key` string, 
  `_hoodie_partition_path` string, 
  `_hoodie_file_name` string, 
  `update_ts_dms` bigint, 
  `emp_no` int, 
  `name` string, 
  `city` string, 
  `salary` int, 
  `schema_name` string)
PARTITIONED BY ( 
  `department` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hudi.hadoop.HoodieParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://hudi-deltastream-apsoutheast1-130835040051/curated/employee_details'
```
