import sys, os, boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark import StorageLevel
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'jdbc_url',
    'rds_username',
    'rds_password'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
s3 = boto3.client('s3')

animal_df = (
    spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .option("mode", "FAILFAST")
    .load("s3://animal-center/stage/animal-center-raw.csv")
)

animal_df.persist(storageLevel=StorageLevel.MEMORY_ONLY)
animal_df = animal_df.dropDuplicates()

animal_df = (
    animal_df
    .withColumnRenamed("Animal ID", "animal_id")
    .withColumnRenamed("Name", "name")
    .withColumnRenamed("DateTime", "datetime")
    .withColumnRenamed("Found Location", "found_location")
    .withColumnRenamed("Intake Type", "intake_type")
    .withColumnRenamed("Intake Condition", "intake_condition")
    .withColumnRenamed("Animal Type", "animal_type")
    .withColumnRenamed("Sex upon Intake", "gender_upon_intake")
    .withColumnRenamed("Age upon Intake", "age_upon_intake")
    .withColumnRenamed("Breed", "breed")
    .withColumnRenamed("Color", "color")
)

animal_df = animal_df.na.fill({"name": "dummy-name"})

def remove_star(name):
    if name and '*' in name:
        return name[1:]
    return name
  
remove_star_udf = udf(remove_star, StringType())
animal_df = animal_df.withColumn("name", remove_star_udf(col("name")))

animal_df = animal_df.withColumn("datetime", 
    to_timestamp(col("datetime"), "MM/dd/yyyy hh:mm:ss a")
)


def convert_to_days(age_string):
    total_days = 0
    parts = age_string.split()
    
    for age in range(0, len(parts), 2):
        if age + 1 >= len(parts):
            break
            
        number = int(parts[age])
        unit = parts[age + 1].lower().rstrip('s')
        
        if unit == 'year':
            total_days += number * 365
        elif unit == 'month':
            total_days += number * 30
        elif unit == 'week':
            total_days += number * 7
        elif unit == 'day':
            total_days += number
            
    return total_days

convert_age_udf = udf(convert_to_days, IntegerType())

animal_df = (
  animal_df
  .withColumn("age_days", convert_age_udf(col("age_upon_intake")))
  .withColumn("age(in years)", round((col("age_days") / 365), 2))
  .drop("age_upon_intake", "MonthYear")
)


animal_df = animal_df.withColumn(
  "age_days",
  when(col("age_days") < 0, col("age_days") * -1)
  .otherwise(col("age_days"))
)

animal_df = animal_df.withColumn(
    "age_category",
    when(col("age_days") <= 365, "Puppy/Kitten")
    .when(col("age_days") <= 365 * 7, "Adult")
    .otherwise("Senior"),
).drop("age_days")

jdbc_url = args['jdbc_url']

connection_properties = {
    "user": args['rds_username'],
    "password": args['rds_password'],
    "batchsize": "10000",                    
    "rewriteBatchedStatements": "true",    
    "useServerPrepStmts": "true",            
    "cachePrepStmts": "true",                
    "useCompression": "true",                
    "socketTimeout": "60000",               
    "autoReconnect": "true",                 
    "useSSL": "false",                       
    "verifyServerCertificate": "false",
}

animal_df.write \
    .mode("overwrite") \
    .option("numPartitions", 10) \
    .option("partitionColumn", "age(in years)") \
    .option("lowerBound", "1") \
    .option("upperBound", "100000") \
    .jdbc(
        url=jdbc_url,
        table="animals",
        properties=connection_properties
    )

(
  animal_df.coalesce(1).write
  .format("csv")
  .option("header","true") 
  .save("s3://animal-center/curated", mode="overwrite")
)

response = s3.list_objects_v2(
    Bucket='animal-center',
    Prefix='curated/'
)

for obj in response['Contents']:
    if 'part-' in obj['Key']:
        old_key = obj['Key']
        break

s3.copy_object(
    Bucket='animal-center',
    CopySource={'Bucket': 'animal-center', 'Key': old_key},
    Key = 'curated/austin-animal-center-processed.csv'  
)

s3.delete_object(
    Bucket='animal-center',
    Key=old_key
)

s3.delete_object(
    Bucket='animal-center',
    Key='stage/animal-center-raw.csv'
)

job.commit()