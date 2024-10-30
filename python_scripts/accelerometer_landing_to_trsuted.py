import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load data from accelerometer_landing
accelerometer_landing = glueContext.create_dynamic_frame.from_catalog(
    database = "stedi_project_db", 
    table_name = "accelerometer_landing"
)

# Load data from customer_trusted
customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database = "stedi_project_db", 
    table_name = "customer_trusted"
)

# Join accelerometer data with customer_trusted on email
accelerometer_trusted = Join.apply(
    frame1 = accelerometer_landing, 
    frame2 = customer_trusted, 
    keys1 = ["user"], 
    keys2 = ["email"]
)

# Write the result to S3 as accelerometer_trusted
glueContext.write_dynamic_frame.from_options(
    frame = accelerometer_trusted,
    connection_type = "s3",
    connection_options = {"path": "s3://jmk-stedi-lakehouse-project/accelerometer_trusted/"},
    format = "json"
)

job.commit()

