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

# Script generated for node accelerometer_landing
accelerometer_landing_node1757015475168 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing_node1757015475168")

# Script generated for node customer_trusted
customer_trusted_node1757015487614 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1757015487614")

# Script generated for node SQL Query
SqlQuery525 = '''
select accelerometer_landing.* from accelerometer_landing 
inner join customer_trusted 
on customer_trusted.email = accelerometer_landing.user

'''
SQLQuery_node1757015498454 = sparkSqlQuery(glueContext, query = SqlQuery525, mapping = {"customer_trusted":customer_trusted_node1757015487614, "accelerometer_landing":accelerometer_landing_node1757015475168}, transformation_ctx = "SQLQuery_node1757015498454")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1757015498454, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1757015466512", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1757015654517 = glueContext.getSink(path="s3://d609-stedi-thill/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1757015654517")
AmazonS3_node1757015654517.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1757015654517.setFormat("json")
AmazonS3_node1757015654517.writeFrame(SQLQuery_node1757015498454)
job.commit()