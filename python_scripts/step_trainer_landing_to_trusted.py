import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load data from step_trainer_landing
step_trainer_landing = glueContext.create_dynamic_frame.from_catalog(
    database = "stedi_project_db", 
    table_name = "step_trainer_landing"
)

# Load data from customers_curated
customers_curated = glueContext.create_dynamic_frame.from_catalog(
    database = "stedi_project_db", 
    table_name = "customers_curated"
)

# Join step_trainer_landing with customers_curated on serialNumber
step_trainer_trusted = Join.apply(
    frame1 = step_trainer_landing, 
    frame2 = customers_curated, 
    keys1 = ["serialNumber"], 
    keys2 = ["serialNumber"]
).drop_fields(["serialnumber", "email", "phone"])

# Write the result to S3 as step_trainer_trusted
glueContext.write_dynamic_frame.from_options(
    frame = step_trainer_trusted,
    connection_type = "s3",
    connection_options = {"path": "s3://jmk-stedi-lakehouse-project/step_trainer_trusted/"},
    format = "json"
)

job.commit()
