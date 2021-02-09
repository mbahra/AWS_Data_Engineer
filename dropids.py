import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "awsdataengineerprojectdatabase", table_name = "teams_fixtures_statistics", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "awsdataengineerprojectdatabase", table_name = "teams_fixtures_statistics", transformation_ctx = "DataSource0")
## @type: DropFields
## @args: [paths = ["idfixture", "idteam"], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame = DataSource0]
Transform0 = DropFields.apply(frame = DataSource0, paths = ["idfixture", "idteam"], transformation_ctx = "Transform0")
# Force one partition, so it can save only 1 file
repartition = Transform0.repartition(1)
## @type: DataSink
## @args: [connection_type = "s3", format = "csv", connection_options = {"path": "s3://dataLakeBucketName/processed-data/api-football/sagemaker-autopilot/", "partitionKeys": []}, transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform0]
dataLakeBucketName = 'XXX'  # Replace XXX by your data lake bucket name
repartition.toDF().write.mode("append").format("csv").option("header", "true").save("s3://" + dataLakeBucketName + "/processed-data/api-football/sagemaker-autopilot/")
job.commit()
