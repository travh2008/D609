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

# Script generated for node customer_curated 
customer_curated_node1757019133079 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customers_curated", transformation_ctx="customer_curated_node1757019133079")

# Script generated for node step_trainer_landing 
step_trainer_landing_node1757019055682 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="step_trainer_landing_node1757019055682")

# Script generated for node SQL Query
SqlQuery473 = '''
select step_trainer_landing.* from step_trainer_landing
inner join customer_curated on customer_curated.serialnumber = step_trainer_landing.serialnumber
'''
SQLQuery_node1757019147859 = sparkSqlQuery(glueContext, query = SqlQuery473, mapping = {"step_trainer_landing":step_trainer_landing_node1757019055682, "customer_curated":customer_curated_node1757019133079}, transformation_ctx = "SQLQuery_node1757019147859")

# Script generated for node step_trainer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1757019147859, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1757018991700", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
step_trainer_trusted_node1757019249546 = glueContext.getSink(path="s3://d609-stedi-thill/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1757019249546")
step_trainer_trusted_node1757019249546.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1757019249546.setFormat("json")
step_trainer_trusted_node1757019249546.writeFrame(SQLQuery_node1757019147859)
job.commit()