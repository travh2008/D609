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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1757019345209 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1757019345209")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1757019346578 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1757019346578")

# Script generated for node SQL Query
SqlQuery501 = '''
select step_trainer_trusted.* , accelerometer_trusted.* from step_trainer_trusted
inner join accelerometer_trusted 
on accelerometer_trusted.timestamp = step_trainer_trusted.sensorreadingtime
'''
SQLQuery_node1757019477575 = sparkSqlQuery(glueContext, query = SqlQuery501, mapping = {"step_trainer_trusted":step_trainer_trusted_node1757019345209, "accelerometer_trusted":accelerometer_trusted_node1757019346578}, transformation_ctx = "SQLQuery_node1757019477575")

# Script generated for node machine_learning_curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1757019477575, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1757018991700", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
machine_learning_curated_node1757019794991 = glueContext.getSink(path="s3://d609-stedi-thill/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1757019794991")
machine_learning_curated_node1757019794991.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
machine_learning_curated_node1757019794991.setFormat("json")
machine_learning_curated_node1757019794991.writeFrame(SQLQuery_node1757019477575)
job.commit()