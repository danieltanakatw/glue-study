import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1715795355466 = glueContext.create_dynamic_frame.from_catalog(database="london-bike", table_name="london_bike", transformation_ctx="AWSGlueDataCatalog_node1715795355466")

# Script generated for node Change Schema
ChangeSchema_node1715796373566 = ApplyMapping.apply(frame=AWSGlueDataCatalog_node1715795355466, mappings=[("number", "long", "number", "long"), ("start date", "string", "start date", "string"), ("start station number", "long", "start station number", "long"), ("start station", "string", "start station", "string"), ("end date", "string", "end date", "string"), ("end station number", "long", "end station number", "long"), ("end station", "string", "end station", "string"), ("bike number", "long", "bike number", "long"), ("bike model", "string", "bike model", "string"), ("total duration", "string", "total duration", "string"), ("`total duration (ms)`", "long", "`total duration (ms)`", "long")], transformation_ctx="ChangeSchema_node1715796373566")

# Script generated for node Amazon S3
AmazonS3_node1715796376778 = glueContext.getSink(path="s3://aws-glue-study-danieltanaka-tw/raw-data/london-bike/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1715796376778")
AmazonS3_node1715796376778.setCatalogInfo(catalogDatabase="london-bike",catalogTableName="london-bike-parquet")
AmazonS3_node1715796376778.setFormat("glueparquet", compression="snappy")
AmazonS3_node1715796376778.writeFrame(ChangeSchema_node1715796373566)
job.commit()