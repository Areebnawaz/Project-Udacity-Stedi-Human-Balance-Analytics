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

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1758803153617 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="Accelerometertrusted_node1758803153617")

# Script generated for node customer trusted
customertrusted_node1758779438609 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customertrusted_node1758779438609")

# Script generated for node Join
Join_node1758779566291 = Join.apply(frame1=Accelerometertrusted_node1758803153617, frame2=customertrusted_node1758779438609, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1758779566291")

# Script generated for node SQL Query
SqlQuery4036 = '''
WITH unique_customers AS (
    SELECT *,
        ROW_number() OVER (
            PARTITION BY email
            ORDER BY email , registrationDate DESC 
        ) AS rn    
    FROM myDataSource
)
SELECT
    customerName,
    email,
    phone,
    birthDay,
    serialNumber,
    registrationDate,
    lastUpdateDate,
    shareWithResearchAsOfDate,
    shareWithPublicAsOfDate,
    shareWithFriendsAsOfDate
FROM unique_customers
WHERE rn = 1 ;
    
 

'''
SQLQuery_node1758779679739 = sparkSqlQuery(glueContext, query = SqlQuery4036, mapping = {"myDataSource":Join_node1758779566291}, transformation_ctx = "SQLQuery_node1758779679739")

# Script generated for node customer curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758779679739, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758777531322", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customercurated_node1758780394452 = glueContext.getSink(path="s3://stedi-human-balance-analytics-2025/customer/customer_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customercurated_node1758780394452")
customercurated_node1758780394452.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
customercurated_node1758780394452.setFormat("json")
customercurated_node1758780394452.writeFrame(SQLQuery_node1758779679739)
job.commit()