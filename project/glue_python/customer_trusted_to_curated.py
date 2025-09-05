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

# Script generated for node customer_trusted
customer_trusted_node1757017283759 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1757017283759")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1757017485447 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1757017485447")

# Script generated for node SQL Query
SqlQuery589 = '''
select distinct customer_trusted.*
from customer_trusted
where customer_trusted.email in (select distinct user from accelerometer_trusted)
'''
SQLQuery_node1757017304007 = sparkSqlQuery(glueContext, query = SqlQuery589, mapping = {"customer_trusted":customer_trusted_node1757017283759, "accelerometer_trusted":accelerometer_trusted_node1757017485447}, transformation_ctx = "SQLQuery_node1757017304007")

# Script generated for node customer_curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1757017304007, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1757016160665", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_curated_node1757017310270 = glueContext.getSink(path="s3://d609-stedi-thill/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_curated_node1757017310270")
customer_curated_node1757017310270.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customers_curated")
customer_curated_node1757017310270.setFormat("json")
customer_curated_node1757017310270.writeFrame(SQLQuery_node1757017304007)
job.commit()