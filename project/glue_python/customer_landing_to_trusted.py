import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# Script generated for node S3 - Customer Landing
S3CustomerLanding_node1757007719505 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="S3CustomerLanding_node1757007719505")

# Script generated for node SQL Query
SqlQuery439 = '''
select * from customer_landing
where shareWithResearchAsOfDate is not null
'''
SQLQuery_node1757014902048 = sparkSqlQuery(glueContext, query = SqlQuery439, mapping = {"customer_landing":S3CustomerLanding_node1757007719505}, transformation_ctx = "SQLQuery_node1757014902048")

# Script generated for node Customer Trusted
CustomerTrusted_node1757010593144 = glueContext.getSink(path="s3://d609-stedi-thill/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1757010593144")
CustomerTrusted_node1757010593144.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
CustomerTrusted_node1757010593144.setFormat("json")
CustomerTrusted_node1757010593144.writeFrame(SQLQuery_node1757014902048)
job.commit()