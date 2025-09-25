import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node accelerometer landing
accelerometerlanding_node1758778443911 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="accelerometerlanding_node1758778443911")

# Script generated for node customer trusted
customertrusted_node1758778445284 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customertrusted_node1758778445284")

# Script generated for node Join
Join_node1758778552309 = Join.apply(frame1=accelerometerlanding_node1758778443911, frame2=customertrusted_node1758778445284, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1758778552309")

# Script generated for node SQL Query
SqlQuery3585 = '''
select user, timestamp, x, y, z from myDataSource ;

'''
SQLQuery_node1758778642724 = sparkSqlQuery(glueContext, query = SqlQuery3585, mapping = {"myDataSource":Join_node1758778552309}, transformation_ctx = "SQLQuery_node1758778642724")

# Script generated for node Drop Duplicates
DropDuplicates_node1758778771593 =  DynamicFrame.fromDF(SQLQuery_node1758778642724.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1758778771593")

# Script generated for node accelerometer trusted
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1758778771593, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758777531322", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometertrusted_node1758778874922 = glueContext.getSink(path="s3://stedi-human-balance-analytics-2025/accelerometer/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometertrusted_node1758778874922")
accelerometertrusted_node1758778874922.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
accelerometertrusted_node1758778874922.setFormat("json")
accelerometertrusted_node1758778874922.writeFrame(DropDuplicates_node1758778771593)
job.commit()