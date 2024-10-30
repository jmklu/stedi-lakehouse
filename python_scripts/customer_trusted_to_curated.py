import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame  # Import DynamicFrame explicitly

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load data from customer_trusted
customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database = "stedi_project_db", 
    table_name = "customer_trusted"
)

# Load data from accelerometer_trusted
accelerometer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database = "stedi_project_db", 
    table_name = "accelerometer_trusted"
)

# Convert DynamicFrames to DataFrames to use dropDuplicates
customer_trusted_df = customer_trusted.toDF().dropDuplicates(["email"])
accelerometer_trusted_df = accelerometer_trusted.toDF().dropDuplicates(["user"])

# Join customer_trusted with accelerometer_trusted on email
customers_curated_df = customer_trusted_df.join(
    accelerometer_trusted_df,
    customer_trusted_df["email"] == accelerometer_trusted_df["user"],
    "inner"
).drop("user")

# Drop any remaining duplicates after the join
customers_curated_df = customers_curated_df.dropDuplicates()

# Convert back to DynamicFrame
customers_curated = DynamicFrame.fromDF(customers_curated_df, glueContext, "customers_curated")

# Write the result to S3 as customers_curated
glueContext.write_dynamic_frame.from_options(
    frame = customers_curated,
    connection_type = "s3",
    connection_options = {"path": "s3://jmk-stedi-lakehouse-project/customers_curated/"},
    format = "json"
)

job.commit()
