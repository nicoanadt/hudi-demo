## EMR Delta Streamer Sample

How to submit delta streamer job to EMR:

```
spark-submit 
--jars /usr/lib/spark/external/lib/spark-avro.jar,/usr/lib/hudi/hudi-spark-bundle.jar --master yarn --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.hive.convertMetastoreParquet=false --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer 
/usr/lib/hudi/hudi-utilities-bundle.jar
--table-type COPY_ON_WRITE --source-ordering-field update_ts_dms --props s3://hudi-deltastream-apsoutheast1-xxxx/config/dfs-source.properties --source-class org.apache.hudi.utilities.sources.ParquetDFSSource --target-base-path s3://hudi-deltastream-apsoutheast1-xxxx/curated/employee_details/ --target-table hudi_deltastream_db.employee_details --transformer-class org.apache.hudi.utilities.transform.SqlQueryBasedTransformer --payload-class org.apache.hudi.common.model.AWSDmsAvroPayload --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider --enable-hive-sync
```

We can use spark-submit command above or submit `Steps` to the EMR Cluster. Sample configuration is available in the `config` folde.
This sample was executed on emr-6.5.0 with Spark 3.1.2, Hive 3.1.2
