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

# Load data from customer_landing
customer_landing = glueContext.create_dynamic_frame.from_catalog(
    database = "stedi_project_db", 
    table_name = "customer_landing"
)

# Filter out customers who didn't consent to share data
customer_trusted = Filter.apply(
    frame = customer_landing, 
    f = lambda x: x["shareWithResearchAsOfDate"] is not None
)

# Write the result to S3 as customer_trusted
glueContext.write_dynamic_frame.from_options(
    frame = customer_trusted,
    connection_type = "s3",
    connection_options = {"path": "s3://jmk-stedi-lakehouse-project/customer_trusted/"},
    format = "json"
)

job.commit()
