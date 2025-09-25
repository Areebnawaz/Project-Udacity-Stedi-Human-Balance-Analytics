import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

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

# Script generated for node step-trainer landing
steptrainerlanding_node1758780869694 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="steptrainerlanding_node1758780869694")

# Script generated for node customer curated
customercurated_node1758780862277 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="customercurated_node1758780862277")

# Script generated for node SQL Query
SqlQuery3790 = '''
SELECT l.* from l 
JOIN c ON c.serialNumber = l.serialNumber

'''
SQLQuery_node1758781200220 = sparkSqlQuery(glueContext, query = SqlQuery3790, mapping = {"c":customercurated_node1758780862277, "l":steptrainerlanding_node1758780869694}, transformation_ctx = "SQLQuery_node1758781200220")

# Script generated for node step-trainer trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758781200220, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758777531322", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
steptrainertrusted_node1758781449728 = glueContext.getSink(path="s3://stedi-human-balance-analytics-2025/step-trainer/step-trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="steptrainertrusted_node1758781449728")
steptrainertrusted_node1758781449728.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
steptrainertrusted_node1758781449728.setFormat("json")
steptrainertrusted_node1758781449728.writeFrame(SQLQuery_node1758781200220)
job.commit()