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
## @args: [database = "awsdataengineerprojectdatabase", table_name = "statistics_75b50a9db7fa0056aa744367aa10eb9e", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "awsdataengineerprojectdatabase", table_name = "statistics_75b50a9db7fa0056aa744367aa10eb9e", transformation_ctx = "DataSource0")
## @type: ApplyMapping
## @args: [mappings = [("idfixture", "long", "(stats) idfixture", "long"), ("idhometeam", "long", "(stats) idhometeam", "long"), ("idawayteam", "long", "(stats) idawayteam", "long"), ("shotsongoalhometeam", "long", "shotsongoalhometeam", "long"), ("shotsongoalawayteam", "long", "shotsongoalawayteam", "long"), ("shotsinsideboxhometeam", "long", "shotsinsideboxhometeam", "long"), ("shotsinsideboxawayteam", "long", "shotsinsideboxawayteam", "long"), ("totalshotshometeam", "long", "totalshotshometeam", "long"), ("totalshotsawayteam", "long", "totalshotsawayteam", "long"), ("ballpossessionhometeam", "string", "ballpossessionhometeam", "string"), ("ballpossessionawayteam", "string", "ballpossessionawayteam", "string")], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame = DataSource0]
Transform0 = ApplyMapping.apply(frame = DataSource0, mappings = [("idfixture", "long", "(stats) idfixture", "long"), ("idhometeam", "long", "(stats) idhometeam", "long"), ("idawayteam", "long", "(stats) idawayteam", "long"), ("shotsongoalhometeam", "long", "shotsongoalhometeam", "long"), ("shotsongoalawayteam", "long", "shotsongoalawayteam", "long"), ("shotsinsideboxhometeam", "long", "shotsinsideboxhometeam", "long"), ("shotsinsideboxawayteam", "long", "shotsinsideboxawayteam", "long"), ("totalshotshometeam", "long", "totalshotshometeam", "long"), ("totalshotsawayteam", "long", "totalshotsawayteam", "long"), ("ballpossessionhometeam", "string", "ballpossessionhometeam", "string"), ("ballpossessionawayteam", "string", "ballpossessionawayteam", "string")], transformation_ctx = "Transform0")
## @type: DataSource
## @args: [database = "awsdataengineerprojectdatabase", table_name = "fixtures_51cfd6df2d3198a05b533fa18a848aa2", transformation_ctx = "DataSource1"]
## @return: DataSource1
## @inputs: []
DataSource1 = glueContext.create_dynamic_frame.from_catalog(database = "awsdataengineerprojectdatabase", table_name = "fixtures_51cfd6df2d3198a05b533fa18a848aa2", transformation_ctx = "DataSource1")
## @type: Join
## @args: [keys2 = ["(stats) idfixture", "(stats) idhometeam", "(stats) idawayteam"], keys1 = ["idfixture", "idhometeam", "idawayteam"], transformation_ctx = "Transform1"]
## @return: Transform1
## @inputs: [frame1 = DataSource1, frame2 = Transform0]
Transform1 = Join.apply(frame1 = DataSource1, frame2 = Transform0, keys2 = ["(stats) idfixture", "(stats) idhometeam", "(stats) idawayteam"], keys1 = ["idfixture", "idhometeam", "idawayteam"], transformation_ctx = "Transform1")
## @type: ApplyMapping
## @args: [mappings = [("idfixture", "long", "idfixture", "int"), ("idhometeam", "long", "idteam", "int"), ("goalshometeam", "long", "goals", "int"), ("shotsongoalhometeam", "long", "shotsongoal", "int"), ("shotsinsideboxhometeam", "long", "shotsinsidebox", "int"), ("totalshotshometeam", "long", "totalshots", "int"), ("ballpossessionhometeam", "string", "ballpossession", "string")], transformation_ctx = "Transform2"]
## @return: Transform2
## @inputs: [frame = Transform1]
Transform2 = ApplyMapping.apply(frame = Transform1, mappings = [("idfixture", "long", "idfixture", "int"), ("idhometeam", "long", "idteam", "int"), ("goalshometeam", "long", "goals", "int"), ("shotsongoalhometeam", "long", "shotsongoal", "int"), ("shotsinsideboxhometeam", "long", "shotsinsidebox", "int"), ("totalshotshometeam", "long", "totalshots", "int"), ("ballpossessionhometeam", "string", "ballpossession", "string")], transformation_ctx = "Transform2")
# Force one partition, so it can save only 1 file
repartition = Transform2.repartition(1)
## @type: DataSink
## @args: [connection_type = "s3", format = "csv", connection_options = {"path": "s3://dataLakeBucketName/processed-data/api-football/teams-fixtures-statistics/", "partitionKeys": []}, transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform2]
dataLakeBucketName = 'XXX' # Replace XXX by your data lake bucket name
repartition.toDF().write.mode("append").format("csv").option("header", "true").save("s3://" + dataLakeBucketName + "/processed-data/api-football/teams-fixtures-statistics/")
job.commit()
