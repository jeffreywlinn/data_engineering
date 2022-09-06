# %%
import urllib
import os
import pyspark as ps
import pandas as pd

# Access our environment variables
ACCESS_KEY = os.environ.get('ACCESS_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')

#Encode the secret key so its not transmitted in plain text
ENCODED_SECRET_KEY = urllib.parse.quote(SECRET_KEY)

# Setup our AWS constants
AWS_S3_BUCKET = 'gharchive-project'
MOUNT_NAME = '/mnt/gharchive'
SOURCE_URL = f"s3n://{ACCESS_KEY}:{ENCODED_SECRET_KEY}@{AWS_S3_BUCKET}"

# %%
# MOUNT THE S3 BRONZE ARCHIVE
#dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# %%
# SHOW THE FOLDERS IN THE S3 BRONZE ARCHIVE
display(dbutils.fs.ls('dbfs:/mnt/gharchive/bronze_layer/year=2020/month=01/'))

# %%
# READ A SINGLE FILE FROM THE S3 BRONZE BUCKET ARCHIVE
df = spark.read.format('json').option("inferSchema", "true").load('dbfs:/mnt/gharchive/bronze_layer/year=2020/month=01/2020-01-01-2.json')

# CACHE DF FOR FURTHER USE BELOW
cacheDF = df.cache()


# %%
# EXPLORE SCHEMA OF SINGLE FILE

df.printSchema()


# %%
# BRONZE CLEANING TO SILVER

from pyspark.sql.functions import *
from pyspark.sql.types import *

# CREATE DATEFRAME FOR THE SILVER LAYER AS DF2 FROM FILE DF
df2 = cacheDF.select(
    col("id").alias("unique_id"),
    col("type").alias("event_type"),
    to_timestamp(col('created_at')).alias("created_at"),
    col("actor.login").alias("actor_login"),
    col("actor.display_login").alias("actor_display"),
    col("org.id").alias("org_id"),
    col("org.login").alias("org_login"),
    col("repo.id").alias("repo_id"),
    col("repo.name").alias("repo_name"))\
    .withColumn('year', year("created_at"))\
    .withColumn('month', month("created_at"))\
    .withColumn('day', dayofmonth("created_at"))

# FILL NULL WITH "NONE"
df2 = df2.na.fill(value="None", subset=["event_type", "actor_login", "actor_display", "org_login", "repo_name"])

# FILL NULL WITH 0
df2 = df2.na.fill(value=0, subset=["repo_id", "org_id", "unique_id"])

df2.show()


# %%
# CREATE DATAFRAME FOR PAYLOAD AS DF3 FROM FILE DF

df3 = cacheDF.select(
    col("id").alias("unique_id"),
    col("type").alias("event_type"),
    to_timestamp(col('created_at')).alias("created_at"),
    col("payload.*"))


# %%
# FROM DF3 FLATTEN ITS NESTED COLUMNS INTO A NEW DF

payload = df3.select(
    "unique_id", "event_type", 
    col("created_at").alias("payload_created_at"), 
    "action", "comment", 
    col("commits.author").alias("commits_author"), 
    col("commits.distinct").alias("commits_distinct"), 
    col("commits.message").alias("commits_message"), 
    col("commits.sha").alias("commits_sha"),
    "description", "master_branch", "member", "pull_request", 
    "push_id", "pusher_type", "ref", "ref_type", "size")
    
# FILL NULL WITH "NONE"
payload = payload.na.fill(value="None", subset=["action", "comment", "commits_author", 
                                                "commits_distinct", "commits_message", 
                                                "commits_sha", "description", "master_branch", 
                                                "member", "pull_request"])

payload = payload.na.fill(value="None", subset=["pusher_type", "ref", "ref_type"])

# FILL NULL WITH 0
payload = payload.na.fill(value=0, subset=["unique_id"])
payload = payload.na.fill(value=0, subset=["push_id"])

payload.show()

# %%
# LEFT OUTER JOIN CACHE DF AND PAYLOAD ON unique_id FOR COMPLETE SILVER TABLE
silver_full = df2.join(other=payload, on=['unique_id', 'event_type'], how='left_outer')

# DROP IRRELEVANT COLUMNS
silver_full = silver_full.drop("payload_created_at", "commits_sha", "member")

display(silver_full)

# %%
# WRITE silver_full DF TO S3 SILVER LAYER (Databricks File Storage)

silver_full.repartition(1).write\
    .mode("overwrite")\
    .format('delta')\
    .save('dbfs:/filestore/shared_uploads/jlinn@skillstorm.com/silver_full/')

# WRITE payload DF TO S3 SILVER LAYER (Databrick File Storage)
payload.write\
    .mode("overwrite")\
    .format("delta")\
    .save('dbfs:/filestore/shared_uploads/jlinn@skillstorm.com/project_2_silver_dim_payload/')

# CLEAR CACHE
cacheDF = cacheDF.unpersist()


# %%
# ENSURE FILE HAS BEEN SAVED TO FILESTORE

file = spark.read.load('dbfs:/filestore/shared_uploads/jlinn@skillstorm.com/silver_full')

display(file)

# %%



