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

# Load data from step_trainer_trusted
step_trainer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database = "stedi_project_db", 
    table_name = "step_trainer_trusted"
)

# Load data from accelerometer_trusted
accelerometer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database = "stedi_project_db", 
    table_name = "accelerometer_trusted"
)

# Join step_trainer_trusted with accelerometer_trusted on timestamp fields
machine_learning_curated = Join.apply(
    frame1 = step_trainer_trusted, 
    frame2 = accelerometer_trusted, 
    keys1 = ["sensorReadingTime"], 
    keys2 = ["timestamp"]
).drop_fields(["timestamp", "user"])

# Write the result to S3 as machine_learning_curated
glueContext.write_dynamic_frame.from_options(
    frame = machine_learning_curated,
    connection_type = "s3",
    connection_options = {"path": "s3://jmk-stedi-lakehouse-project/machine_learning_curated/"},
    format = "json"
)

job.commit()
