import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "awsdataengineerprojectdatabase", table_name = "fixtures_51cfd6df2d3198a05b533fa18a848aa2", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "awsdataengineerprojectdatabase", table_name = "fixtures_51cfd6df2d3198a05b533fa18a848aa2", transformation_ctx = "DataSource0")
## @type: DataSource
## @args: [database = "awsdataengineerprojectdatabase", table_name = "xgoals_predictions", transformation_ctx = "DataSource2"]
## @return: DataSource2
## @inputs: []
DataSource2 = glueContext.create_dynamic_frame.from_catalog(database = "awsdataengineerprojectdatabase", table_name = "xgoals_predictions", transformation_ctx = "DataSource2")
## @type: Join
## @args: [keys2 = ["idfixture", "idawayteam"], keys1 = ["col0", "col1"], transformation_ctx = "Transform7"]
## @return: Transform7
## @inputs: [frame1 = DataSource2, frame2 = DataSource0]
Transform7 = Join.apply(frame1 = DataSource2, frame2 = DataSource0, keys2 = ["idfixture", "idawayteam"], keys1 = ["col0", "col1"], transformation_ctx = "Transform7")
## @type: ApplyMapping
## @args: [mappings = [("col7", "double", "xgoalsawayteam", "double"), ("idfixture", "long", "idfixture", "long"), ("status", "string", "status", "string"), ("date", "string", "date", "string"), ("time", "string", "time", "string"), ("idhometeam", "long", "idhometeam", "long"), ("idawayteam", "long", "idawayteam", "long"), ("goalshometeam", "long", "goalshometeam", "long"), ("goalsawayteam", "long", "goalsawayteam", "long")], transformation_ctx = "Transform1"]
## @return: Transform1
## @inputs: [frame = Transform7]
Transform1 = ApplyMapping.apply(frame = Transform7, mappings = [("col7", "double", "xgoalsawayteam", "double"), ("idfixture", "long", "idfixture", "long"), ("status", "string", "status", "string"), ("date", "string", "date", "string"), ("time", "string", "time", "string"), ("idhometeam", "long", "idhometeam", "long"), ("idawayteam", "long", "idawayteam", "long"), ("goalshometeam", "long", "goalshometeam", "long"), ("goalsawayteam", "long", "goalsawayteam", "long")], transformation_ctx = "Transform1")
## @type: ApplyMapping
## @args: [mappings = [("xgoalsawayteam", "double", "(right) xgoalsawayteam", "double"), ("idfixture", "long", "(right) idfixture", "long"), ("status", "string", "(right) status", "string"), ("date", "string", "(right) date", "string"), ("time", "string", "(right) time", "string"), ("idhometeam", "long", "(right) idhometeam", "long"), ("idawayteam", "long", "(right) idawayteam", "long"), ("goalshometeam", "long", "(right) goalshometeam", "long"), ("goalsawayteam", "long", "(right) goalsawayteam", "long")], transformation_ctx = "Transform2"]
## @return: Transform2
## @inputs: [frame = Transform1]
Transform2 = ApplyMapping.apply(frame = Transform1, mappings = [("xgoalsawayteam", "double", "(right) xgoalsawayteam", "double"), ("idfixture", "long", "(right) idfixture", "long"), ("status", "string", "(right) status", "string"), ("date", "string", "(right) date", "string"), ("time", "string", "(right) time", "string"), ("idhometeam", "long", "(right) idhometeam", "long"), ("idawayteam", "long", "(right) idawayteam", "long"), ("goalshometeam", "long", "(right) goalshometeam", "long"), ("goalsawayteam", "long", "(right) goalsawayteam", "long")], transformation_ctx = "Transform2")
## @type: Join
## @args: [columnConditions = ["=", "="], joinType = left, keys2 = ["col0", "col1"], keys1 = ["idfixture", "idhometeam"], transformation_ctx = "Transform4"]
## @return: Transform4
## @inputs: [frame1 = DataSource0, frame2 = DataSource2]
DataSource0DF = DataSource0.toDF()
DataSource2DF = DataSource2.toDF()
Transform4 = DynamicFrame.fromDF(DataSource0DF.join(DataSource2DF, (DataSource0DF['idfixture'] == DataSource2DF['col0']) & (DataSource0DF['idhometeam'] == DataSource2DF['col1']), "left"), glueContext, "Transform4")
## @type: ApplyMapping
## @args: [mappings = [("idfixture", "long", "idfixture", "long"), ("status", "string", "status", "string"), ("date", "string", "date", "string"), ("time", "string", "time", "string"), ("idhometeam", "long", "idhometeam", "long"), ("idawayteam", "long", "idawayteam", "long"), ("goalshometeam", "long", "goalshometeam", "long"), ("goalsawayteam", "long", "goalsawayteam", "long"), ("col7", "double", "xgoalshometeam", "double")], transformation_ctx = "Transform3"]
## @return: Transform3
## @inputs: [frame = Transform4]
Transform3 = ApplyMapping.apply(frame = Transform4, mappings = [("idfixture", "long", "idfixture", "long"), ("status", "string", "status", "string"), ("date", "string", "date", "string"), ("time", "string", "time", "string"), ("idhometeam", "long", "idhometeam", "long"), ("idawayteam", "long", "idawayteam", "long"), ("goalshometeam", "long", "goalshometeam", "long"), ("goalsawayteam", "long", "goalsawayteam", "long"), ("col7", "double", "xgoalshometeam", "double")], transformation_ctx = "Transform3")
## @type: Join
## @args: [columnConditions = ["="], joinType = left, keys2 = ["(right) idfixture"], keys1 = ["idfixture"], transformation_ctx = "Transform8"]
## @return: Transform8
## @inputs: [frame1 = Transform3, frame2 = Transform2]
Transform3DF = Transform3.toDF()
Transform2DF = Transform2.toDF()
Transform8 = DynamicFrame.fromDF(Transform3DF.join(Transform2DF, (Transform3DF['idfixture'] == Transform2DF['(right) idfixture']), "left"), glueContext, "Transform8")
## @type: ApplyMapping
## @args: [mappings = [("idfixture", "long", "idfixture", "long"), ("status", "string", "status", "string"), ("date", "string", "date", "string"), ("time", "string", "time", "string"), ("idhometeam", "long", "idhometeam", "long"), ("idawayteam", "long", "idawayteam", "long"), ("goalshometeam", "long", "goalshometeam", "long"), ("goalsawayteam", "long", "goalsawayteam", "long"), ("xgoalshometeam", "double", "xgoalshometeam", "double"), ("(right) xgoalsawayteam", "double", "xgoalsawayteam", "double")], transformation_ctx = "Transform5"]
## @return: Transform5
## @inputs: [frame = Transform8]
Transform5 = ApplyMapping.apply(frame = Transform8, mappings = [("idfixture", "long", "idfixture", "long"), ("status", "string", "status", "string"), ("date", "string", "date", "string"), ("time", "string", "time", "string"), ("idhometeam", "long", "idhometeam", "long"), ("idawayteam", "long", "idawayteam", "long"), ("goalshometeam", "long", "goalshometeam", "long"), ("goalsawayteam", "long", "goalsawayteam", "long"), ("xgoalshometeam", "double", "xgoalshometeam", "double"), ("(right) xgoalsawayteam", "double", "xgoalsawayteam", "double")], transformation_ctx = "Transform5")
## @type: DataSource
## @args: [database = "awsdataengineerprojectdatabase", table_name = "statistics_75b50a9db7fa0056aa744367aa10eb9e", transformation_ctx = "DataSource1"]
## @return: DataSource1
## @inputs: []
DataSource1 = glueContext.create_dynamic_frame.from_catalog(database = "awsdataengineerprojectdatabase", table_name = "statistics_75b50a9db7fa0056aa744367aa10eb9e", transformation_ctx = "DataSource1")
## @type: ApplyMapping
## @args: [mappings = [("idfixture", "long", "(right) idfixture", "long"), ("idhometeam", "long", "(right) idhometeam", "long"), ("idawayteam", "long", "(right) idawayteam", "long"), ("shotsongoalhometeam", "long", "(right) shotsongoalhometeam", "long"), ("shotsongoalawayteam", "long", "(right) shotsongoalawayteam", "long"), ("shotsinsideboxhometeam", "long", "(right) shotsinsideboxhometeam", "long"), ("shotsinsideboxawayteam", "long", "(right) shotsinsideboxawayteam", "long"), ("totalshotshometeam", "long", "(right) totalshotshometeam", "long"), ("totalshotsawayteam", "long", "(right) totalshotsawayteam", "long"), ("ballpossessionhometeam", "string", "(right) ballpossessionhometeam", "string"), ("ballpossessionawayteam", "string", "(right) ballpossessionawayteam", "string")], transformation_ctx = "Transform9"]
## @return: Transform9
## @inputs: [frame = DataSource1]
Transform9 = ApplyMapping.apply(frame = DataSource1, mappings = [("idfixture", "long", "(right) idfixture", "long"), ("idhometeam", "long", "(right) idhometeam", "long"), ("idawayteam", "long", "(right) idawayteam", "long"), ("shotsongoalhometeam", "long", "(right) shotsongoalhometeam", "long"), ("shotsongoalawayteam", "long", "(right) shotsongoalawayteam", "long"), ("shotsinsideboxhometeam", "long", "(right) shotsinsideboxhometeam", "long"), ("shotsinsideboxawayteam", "long", "(right) shotsinsideboxawayteam", "long"), ("totalshotshometeam", "long", "(right) totalshotshometeam", "long"), ("totalshotsawayteam", "long", "(right) totalshotsawayteam", "long"), ("ballpossessionhometeam", "string", "(right) ballpossessionhometeam", "string"), ("ballpossessionawayteam", "string", "(right) ballpossessionawayteam", "string")], transformation_ctx = "Transform9")
## @type: Join
## @args: [columnConditions = ["="], joinType = left, keys2 = ["(right) idfixture"], keys1 = ["idfixture"], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame1 = Transform5, frame2 = Transform9]
Transform5DF = Transform5.toDF()
Transform9DF = Transform9.toDF()
Transform0 = DynamicFrame.fromDF(Transform5DF.join(Transform9DF, (Transform5DF['idfixture'] == Transform9DF['(right) idfixture']), "left"), glueContext, "Transform0")
## @type: ApplyMapping
## @args: [mappings = [("idfixture", "long", "idfixture", "long"), ("status", "string", "status", "string"), ("date", "string", "date", "string"), ("time", "string", "time", "string"), ("idhometeam", "long", "idhometeam", "long"), ("idawayteam", "long", "idawayteam", "long"), ("goalshometeam", "long", "goalshometeam", "long"), ("goalsawayteam", "long", "goalsawayteam", "long"), ("xgoalshometeam", "double", "xgoalshometeam", "double"), ("xgoalsawayteam", "double", "xgoalsawayteam", "double"), ("(right) shotsongoalhometeam", "long", "shotsongoalhometeam", "long"), ("(right) shotsongoalawayteam", "long", "shotsongoalawayteam", "long"), ("(right) shotsinsideboxhometeam", "long", "shotsinsideboxhometeam", "long"), ("(right) shotsinsideboxawayteam", "long", "shotsinsideboxawayteam", "long"), ("(right) totalshotshometeam", "long", "totalshotshometeam", "long"), ("(right) totalshotsawayteam", "long", "totalshotsawayteam", "long"), ("(right) ballpossessionhometeam", "string", "ballpossessionhometeam", "string"), ("(right) ballpossessionawayteam", "string", "ballpossessionawayteam", "string")], transformation_ctx = "Transform6"]
## @return: Transform6
## @inputs: [frame = Transform0]
Transform6 = ApplyMapping.apply(frame = Transform0, mappings = [("idfixture", "long", "idfixture", "long"), ("status", "string", "status", "string"), ("date", "string", "date", "string"), ("time", "string", "time", "string"), ("idhometeam", "long", "idhometeam", "long"), ("idawayteam", "long", "idawayteam", "long"), ("goalshometeam", "long", "goalshometeam", "long"), ("goalsawayteam", "long", "goalsawayteam", "long"), ("xgoalshometeam", "double", "xgoalshometeam", "double"), ("xgoalsawayteam", "double", "xgoalsawayteam", "double"), ("(right) shotsongoalhometeam", "long", "shotsongoalhometeam", "long"), ("(right) shotsongoalawayteam", "long", "shotsongoalawayteam", "long"), ("(right) shotsinsideboxhometeam", "long", "shotsinsideboxhometeam", "long"), ("(right) shotsinsideboxawayteam", "long", "shotsinsideboxawayteam", "long"), ("(right) totalshotshometeam", "long", "totalshotshometeam", "long"), ("(right) totalshotsawayteam", "long", "totalshotsawayteam", "long"), ("(right) ballpossessionhometeam", "string", "ballpossessionhometeam", "string"), ("(right) ballpossessionawayteam", "string", "ballpossessionawayteam", "string")], transformation_ctx = "Transform6")
## @type: DataSink
## @args: [connection_type = "s3", format = "csv", connection_options = {"path": "s3://datalake-57e3d8aa-01ab-432f-8b25-78d63bb86886/processed-data/api-football/", "partitionKeys": []}, transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform6]
DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform6, connection_type = "s3", format = "csv", connection_options = {"path": "s3://datalake-57e3d8aa-01ab-432f-8b25-78d63bb86886/processed-data/api-football/", "partitionKeys": []}, transformation_ctx = "DataSink0")
job.commit()
