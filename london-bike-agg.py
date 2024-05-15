import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import udf


glueContext = GlueContext(SparkContext.getOrCreate())

london_bike_dynamicframe = glueContext.create_dynamic_frame.from_catalog(
       database = "london-bike",
       table_name = "london_bike")
london_bike_dynamicframe.printSchema()
london_bike_df = london_bike_dynamicframe.toDF()
london_bike_df_agg = london_bike_df.groupBy("bike_number") \
    .agg(sum("total_duration__ms_#9").alias("sum_total_duration__ms")
     )

london_bike_dyf_agg = DynamicFrame.fromDF(london_bike_df_agg, glueContext)

glueContext.write_dynamic_frame.from_options(
       frame = london_bike_dyf_agg,
       connection_type = "s3",
       connection_options = {"path": "s3://aws-glue-study-danieltanaka-tw/aggregated-data/london-bike/"},
       format = "parquet")