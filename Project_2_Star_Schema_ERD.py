# %%
# CREATING THE ERD, STAR/SNOWFLAKE SCHEMAS FOR SILVER TO GOLD
import urllib
import os
import pyspark as ps
import pandas as pd

# Access our environment variables
ACCESS_KEY = os.environ.get('ACCESS_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')

#Encode the secret key so its not transmitted in plain text
ENCODED_SECRET_KEY = urllib.parse.quote(SECRET_KEY)

# Setup our SILVER AWS constants
AWS_S3_SILVER_BUCKET = 'gharchive-silver'
SILVER_MOUNT_NAME = '/mnt/gharchive-silver'
SILVER_SOURCE_URL = f"s3n://{ACCESS_KEY}:{ENCODED_SECRET_KEY}@{AWS_S3_SILVER_BUCKET}"


# %%
# MOUNT SILVER BUCKET
# dbutils.fs.mount(SILVER_SOURCE_URL, SILVER_MOUNT_NAME)

# %%
# READ A FILE FROM SILVER LAYER S3 (Filestore)
df = spark.read.format("delta").load('dbfs:/filestore/shared_uploads/jlinn@skillstorm.com/silver_full/')

# PUT IN MEMORY FOR FURTHER USE
cacheDF = df.cache()

# DISPLAY THE SILVER FILE DF
display(cacheDF)

# %%
# FILL NULL WITH "None"
silver_fact = cacheDF.fillna(value="None")
silver_fact = cacheDF.fillna(value=0)

# DROP IRRELEVANT COLUMNS
silver_fact = cacheDF.drop("action", "actor_display", "org_login", 
                           "repo_name", "comment", "description", 
                           "master_branch", "pull_request", "pusher_type")

# REORGANIZE COLUMNS
silver_fact = cacheDF.select("unique_id", "event_type", "created_at", 
                             "year", "month", "day", "actor_login", 
                             "org_id", "repo_id", "push_id", 
                             "commits_author", "commits_distinct", 
                             "commits_message", 
                             "ref", "ref_type", "size")

silver_fact.show()

# %%
# WRITE silver_fact DF TO FILESTORE AS SILVER LAYER FACT TABLE
silver_fact.write\
    .format("delta")\
    .mode("overwrite")\
    .save('dbfs:/mnt/gharchive-gold/trainee=JeffreyLinn/silver_fact_table/')

# %%
from pyspark.sql.functions import *
from pyspark.sql.types import *

# BREAK OUT SILVER DIMENSIONS OF STAR SCHEMA: event, actor, org, repo, commits, payload :

# silver_dim_event
silver_dim_event = cacheDF.select("unique_id", "event_type", "created_at", "year", "month", "day")

# silver_dim_actor
silver_dim_actor = cacheDF.select("actor_login", "actor_display")

# silver_dim_org
silver_dim_org = cacheDF.select("org_id", "org_login")

# silver_dim_repo
silver_dim_repo = cacheDF.select("repo_id", "repo_name")

# silver_dim_commits
silver_dim_commits = cacheDF.select("push_id", "commits_author", "commits_distinct", "commits_message", "ref", "ref_type", "size")

# silver_dim_payload
silver_dim_payload = cacheDF.select("push_id", "action", "comment", "description", "master_branch", "pull_request")


# %%
silver_dim_actor.show()

# %%
silver_dim_org.show()

# %%
silver_dim_repo.show()

# %%
# WRITE EACH DIMENSION TABLE TO FILE STORE

silver_dim_actor.write.format("delta").mode("overwrite").save('dbfs:/mnt/gharchive-gold/trainee=JeffreyLinn/dimension=actor/')
silver_dim_org.write.format("delta").mode("overwrite").save('dbfs:/mnt/gharchive-gold/trainee=JeffreyLinn/dimension=org/')
silver_dim_repo.write.format("delta").mode("overwrite").save('dbfs:/mnt/gharchive-gold/trainee=JeffreyLinn/dimension=repo/')
silver_dim_payload.write.format("delta").mode("overwrite").save('dbfs:/mnt/gharchive-gold/trainee=JeffreyLinn/dimension=payload/')
silver_dim_commits.write.format("delta").mode("overwrite").save('dbfs:/mnt/gharchive-gold/trainee=JeffreyLinn/dimension=commits/')


# %%



