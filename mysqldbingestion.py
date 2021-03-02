import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "awsdataengineerprojectdatabase", table_name = "xgoals_predictions", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "awsdataengineerprojectdatabase", table_name = "xgoals_predictions", transformation_ctx = "DataSource0")
## @type: ApplyMapping
## @args: [mappings = [("col0", "string", "(predictions) idfixture", "int"), ("col1", "string", "(predictions) idteam", "int"), ("col2", "double", "(predictions) xgoals", "double")], transformation_ctx = "Transform1"]
## @return: Transform1
## @inputs: [frame = DataSource0]
Transform1 = ApplyMapping.apply(frame = DataSource0, mappings = [("col0", "string", "(predictions) idfixture", "int"), ("col1", "string", "(predictions) idteam", "int"), ("col2", "double", "(predictions) xgoals", "double")], transformation_ctx = "Transform1")
## @type: DataSource
## @args: [database = "awsdataengineerprojectdatabase", table_name = "statistics_75b50a9db7fa0056aa744367aa10eb9e", transformation_ctx = "DataSource2"]
## @return: DataSource2
## @inputs: []
DataSource2 = glueContext.create_dynamic_frame.from_catalog(database = "awsdataengineerprojectdatabase", table_name = "statistics_75b50a9db7fa0056aa744367aa10eb9e", transformation_ctx = "DataSource2")
## @type: ApplyMapping
## @args: [mappings = [("idfixture", "long", "(stats) idfixture", "int"), ("shotsongoalhometeam", "long", "shotsongoalhometeam", "int"), ("shotsongoalawayteam", "long", "shotsongoalawayteam", "int"), ("shotsinsideboxhometeam", "long", "shotsinsideboxhometeam", "int"), ("shotsinsideboxawayteam", "long", "shotsinsideboxawayteam", "int"), ("totalshotshometeam", "long", "totalshotshometeam", "int"), ("totalshotsawayteam", "long", "totalshotsawayteam", "int"), ("ballpossessionhometeam", "string", "ballpossessionhometeam", "string"), ("ballpossessionawayteam", "string", "ballpossessionawayteam", "string")], transformation_ctx = "Transform3"]
## @return: Transform3
## @inputs: [frame = DataSource2]
Transform3 = ApplyMapping.apply(frame = DataSource2, mappings = [("idfixture", "long", "(stats) idfixture", "int"), ("shotsongoalhometeam", "long", "shotsongoalhometeam", "int"), ("shotsongoalawayteam", "long", "shotsongoalawayteam", "int"), ("shotsinsideboxhometeam", "long", "shotsinsideboxhometeam", "int"), ("shotsinsideboxawayteam", "long", "shotsinsideboxawayteam", "int"), ("totalshotshometeam", "long", "totalshotshometeam", "int"), ("totalshotsawayteam", "long", "totalshotsawayteam", "int"), ("ballpossessionhometeam", "string", "ballpossessionhometeam", "string"), ("ballpossessionawayteam", "string", "ballpossessionawayteam", "string")], transformation_ctx = "Transform3")
## @type: DataSource
## @args: [database = "awsdataengineerprojectdatabase", table_name = "fixtures_51cfd6df2d3198a05b533fa18a848aa2", transformation_ctx = "DataSource1"]
## @return: DataSource1
## @inputs: []
DataSource1 = glueContext.create_dynamic_frame.from_catalog(database = "awsdataengineerprojectdatabase", table_name = "fixtures_51cfd6df2d3198a05b533fa18a848aa2", transformation_ctx = "DataSource1")
## @type: Join
## @args: [keys2 = ["idfixture"], keys1 = ["(stats) idfixture"], transformation_ctx = "Transform7"]
## @return: Transform7
## @inputs: [frame1 = Transform3, frame2 = DataSource1]
Transform7 = Join.apply(frame1 = Transform3, frame2 = DataSource1, keys2 = ["idfixture"], keys1 = ["(stats) idfixture"], transformation_ctx = "Transform7")
## @type: Filter
## @args: [f = lambda row : (bool(re.match("Match Finished", row["status"]))), transformation_ctx = "Transform2"]
## @return: Transform2
## @inputs: [frame = Transform7]
Transform2 = Filter.apply(frame = Transform7, f = lambda row : (bool(re.match("Match Finished", row["status"]))), transformation_ctx = "Transform2")
## @type: ApplyMapping
## @args: [mappings = [("shotsongoalhometeam", "int", "shotsongoalhometeam", "int"), ("shotsongoalawayteam", "int", "shotsongoalawayteam", "int"), ("shotsinsideboxhometeam", "int", "shotsinsideboxhometeam", "int"), ("shotsinsideboxawayteam", "int", "shotsinsideboxawayteam", "int"), ("totalshotshometeam", "int", "totalshotshometeam", "int"), ("totalshotsawayteam", "int", "totalshotsawayteam", "int"), ("ballpossessionhometeam", "string", "ballpossessionhometeam", "string"), ("ballpossessionawayteam", "string", "ballpossessionawayteam", "string"), ("idfixture", "long", "idfixture", "int"), ("date", "string", "date", "string"), ("time", "string", "time", "string"), ("idhometeam", "long", "idhometeam", "int"), ("idawayteam", "long", "idawayteam", "int"), ("goalshometeam", "long", "goalshometeam", "int"), ("goalsawayteam", "long", "goalsawayteam", "int")], transformation_ctx = "Transform6"]
## @return: Transform6
## @inputs: [frame = Transform2]
Transform6 = ApplyMapping.apply(frame = Transform2, mappings = [("shotsongoalhometeam", "int", "shotsongoalhometeam", "int"), ("shotsongoalawayteam", "int", "shotsongoalawayteam", "int"), ("shotsinsideboxhometeam", "int", "shotsinsideboxhometeam", "int"), ("shotsinsideboxawayteam", "int", "shotsinsideboxawayteam", "int"), ("totalshotshometeam", "int", "totalshotshometeam", "int"), ("totalshotsawayteam", "int", "totalshotsawayteam", "int"), ("ballpossessionhometeam", "string", "ballpossessionhometeam", "string"), ("ballpossessionawayteam", "string", "ballpossessionawayteam", "string"), ("idfixture", "long", "idfixture", "int"), ("date", "string", "date", "string"), ("time", "string", "time", "string"), ("idhometeam", "long", "idhometeam", "int"), ("idawayteam", "long", "idawayteam", "int"), ("goalshometeam", "long", "goalshometeam", "int"), ("goalsawayteam", "long", "goalsawayteam", "int")], transformation_ctx = "Transform6")
## @type: Join
## @args: [columnConditions = ["=", "="], joinType = right, keys2 = ["idfixture", "idhometeam"], keys1 = ["(predictions) idfixture", "(predictions) idteam"], transformation_ctx = "Transform4"]
## @return: Transform4
## @inputs: [frame1 = Transform1, frame2 = Transform6]
Transform1DF = Transform1.toDF()
Transform6DF = Transform6.toDF()
Transform4 = DynamicFrame.fromDF(Transform1DF.join(Transform6DF, (Transform1DF['(predictions) idfixture'] == Transform6DF['idfixture']) & (Transform1DF['(predictions) idteam'] == Transform6DF['idhometeam']), "right"), glueContext, "Transform4")
## @type: ApplyMapping
## @args: [mappings = [("(predictions) xgoals", "double", "xgoalshometeam", "double"), ("shotsongoalhometeam", "int", "shotsongoalhometeam", "int"), ("shotsongoalawayteam", "int", "shotsongoalawayteam", "int"), ("shotsinsideboxhometeam", "int", "shotsinsideboxhometeam", "int"), ("shotsinsideboxawayteam", "int", "shotsinsideboxawayteam", "int"), ("totalshotshometeam", "int", "totalshotshometeam", "int"), ("totalshotsawayteam", "int", "totalshotsawayteam", "int"), ("ballpossessionhometeam", "string", "ballpossessionhometeam", "string"), ("ballpossessionawayteam", "string", "ballpossessionawayteam", "string"), ("idfixture", "int", "idfixture", "int"), ("date", "string", "date", "string"), ("time", "string", "time", "string"), ("idhometeam", "int", "idhometeam", "int"), ("idawayteam", "int", "idawayteam", "int"), ("goalshometeam", "int", "goalshometeam", "int"), ("goalsawayteam", "int", "goalsawayteam", "int")], transformation_ctx = "Transform5"]
## @return: Transform5
## @inputs: [frame = Transform4]
Transform5 = ApplyMapping.apply(frame = Transform4, mappings = [("(predictions) xgoals", "double", "xgoalshometeam", "double"), ("shotsongoalhometeam", "int", "shotsongoalhometeam", "int"), ("shotsongoalawayteam", "int", "shotsongoalawayteam", "int"), ("shotsinsideboxhometeam", "int", "shotsinsideboxhometeam", "int"), ("shotsinsideboxawayteam", "int", "shotsinsideboxawayteam", "int"), ("totalshotshometeam", "int", "totalshotshometeam", "int"), ("totalshotsawayteam", "int", "totalshotsawayteam", "int"), ("ballpossessionhometeam", "string", "ballpossessionhometeam", "string"), ("ballpossessionawayteam", "string", "ballpossessionawayteam", "string"), ("idfixture", "int", "idfixture", "int"), ("date", "string", "date", "string"), ("time", "string", "time", "string"), ("idhometeam", "int", "idhometeam", "int"), ("idawayteam", "int", "idawayteam", "int"), ("goalshometeam", "int", "goalshometeam", "int"), ("goalsawayteam", "int", "goalsawayteam", "int")], transformation_ctx = "Transform5")
## @type: Join
## @args: [columnConditions = ["=", "="], joinType = left, keys2 = ["(predictions) idfixture", "(predictions) idteam"], keys1 = ["idfixture", "idawayteam"], transformation_ctx = "Transform8"]
## @return: Transform8
## @inputs: [frame1 = Transform5, frame2 = Transform1]
Transform5DF = Transform5.toDF()
Transform1DF = Transform1.toDF()
Transform8 = DynamicFrame.fromDF(Transform5DF.join(Transform1DF, (Transform5DF['idfixture'] == Transform1DF['(predictions) idfixture']) & (Transform5DF['idawayteam'] == Transform1DF['(predictions) idteam']), "left"), glueContext, "Transform8")
## @type: ApplyMapping
## @args: [mappings = [("date", "string", "date", "string"), ("(predictions) xgoals", "double", "xgoalsawayteam", "decimal"), ("shotsinsideboxhometeam", "int", "shotsinsideboxhometeam", "int"), ("totalshotsawayteam", "int", "totalshotsawayteam", "int"), ("totalshotshometeam", "int", "totalshotshometeam", "int"), ("xgoalshometeam", "double", "xgoalshometeam", "decimal"), ("idfixture", "int", "idfixture", "int"), ("goalshometeam", "int", "goalshometeam", "int"), ("idawayteam", "int", "idawayteam", "int"), ("goalsawayteam", "int", "goalsawayteam", "int"), ("ballpossessionhometeam", "string", "ballpossessionhometeam", "string"), ("idhometeam", "int", "idhometeam", "int"), ("shotsongoalhometeam", "int", "shotsongoalhometeam", "int"), ("shotsinsideboxawayteam", "int", "shotsinsideboxawayteam", "int"), ("time", "string", "time", "string"), ("shotsongoalawayteam", "int", "shotsongoalawayteam", "int"), ("ballpossessionawayteam", "string", "ballpossessionawayteam", "string")], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame = Transform8]
Transform0 = ApplyMapping.apply(frame = Transform8, mappings = [("date", "string", "date", "string"), ("(predictions) xgoals", "double", "xgoalsawayteam", "decimal"), ("shotsinsideboxhometeam", "int", "shotsinsideboxhometeam", "int"), ("totalshotsawayteam", "int", "totalshotsawayteam", "int"), ("totalshotshometeam", "int", "totalshotshometeam", "int"), ("xgoalshometeam", "double", "xgoalshometeam", "decimal"), ("idfixture", "int", "idfixture", "int"), ("goalshometeam", "int", "goalshometeam", "int"), ("idawayteam", "int", "idawayteam", "int"), ("goalsawayteam", "int", "goalsawayteam", "int"), ("ballpossessionhometeam", "string", "ballpossessionhometeam", "string"), ("idhometeam", "int", "idhometeam", "int"), ("shotsongoalhometeam", "int", "shotsongoalhometeam", "int"), ("shotsinsideboxawayteam", "int", "shotsinsideboxawayteam", "int"), ("time", "string", "time", "string"), ("shotsongoalawayteam", "int", "shotsongoalawayteam", "int"), ("ballpossessionawayteam", "string", "ballpossessionawayteam", "string")], transformation_ctx = "Transform0")
## @type: DataSink
## @args: [database = "awsdataengineerprojectdatabase", table_name = "mysqldb_fixtures", transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform0]
DataSink0 = glueContext.write_dynamic_frame.from_catalog(frame = Transform0, database = "awsdataengineerprojectdatabase", table_name = "mysqldb_fixtures", transformation_ctx = "DataSink0")
job.commit()
