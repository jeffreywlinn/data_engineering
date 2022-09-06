# %%
import urllib
import os
import pyspark as ps
import pandas as pd
from pyspark.sql.functions import *

# Access our environment variables
ACCESS_KEY = os.environ.get('ACCESS_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')

#Encode the secret key so its not transmitted in plain text
ENCODED_SECRET_KEY = urllib.parse.quote(SECRET_KEY)

# Setup our SILVER AWS constants
AWS_S3_SILVER_BUCKET = 'gharchive-silver'
SILVER_MOUNT_NAME = '/mnt/gharchive-silver'
SILVER_SOURCE_URL = f"s3n://{ACCESS_KEY}:{ENCODED_SECRET_KEY}@{AWS_S3_SILVER_BUCKET}"

# Setup our GOLD AWS constants
AWS_S3_GOLD_BUCKET = 'gharchive-gold'
GOLD_MOUNT_NAME = '/mnt/gharchive-gold'
GOLD_SOURCE_URL = f"s3n://{ACCESS_KEY}:{ENCODED_SECRET_KEY}@{AWS_S3_GOLD_BUCKET}"


# %%
# MOUNT SILVER BUCKET
# dbutils.fs.mount(SILVER_SOURCE_URL, SILVER_MOUNT_NAME)

# MOUNT GOLD BUCKET
# dbutils.fs.mount(GOLD_SOURCE_URL, GOLD_MOUNT_NAME)

# %%
# SHOW THE FACT & DIMENSION FILES IN THE S3 SILVER ARCHIVE (Filestore)
display(dbutils.fs.ls('dbfs:/filestore/shared_uploads/jlinn@skillstorm.com/'))


# %%
# READ A SINGLE FILE FROM THE S3 SILVER BUCKET ARCHIVE (Silver Fact Table)
df = spark.read.format("delta")\
    .load('dbfs:/filestore/shared_uploads/jlinn@skillstorm.com/silver_full/')

# CACHE FOR FUTURE USE
cacheDF = df.cache()

# %%
#**********************************************************************

# SILVER TO GOLD AGGREGATION REQUIREMENTS FOR SPARROW ANALYTICS
# 1) Data aggregated by type of GitHub event per hour
# 2) Data aggregated per user
# 3) Breakdown of number of commits per PushEvent
# 4) Challenge: Based on the commit messages – breakdown the events by language
#**********************************************************************

# %%
# 1) DATA AGGREGATED BY TYPE OF EVENT PER HOUR
event_type_per_hour = cacheDF.withColumn('hour',hour(cacheDF.created_at))
event_type_per_hour.select('unique_id', 'event_type', 'created_at', 'year', 'month', 'day', 'hour')\
    .groupBy('year', 'month', 'day', 'hour', 'event_type')\
    .count()\
    .sort(desc('count'))\
    .show()


# %%
# WRITE AGGREGATION #1 TO DELTA GOLD PARTITION BY YEAR MONTH DAY HOUR
event_type_per_hour.write\
    .partitionBy('year', 'month', 'day', 'hour')\
    .mode('overwrite')\
    .format('delta')\
    .save('dbfs:/mnt/gharchive-gold/trainee=JeffreyLinn/event_type_per_hour/')


# %%
# 2) DATA AGGREGATED PER USER
agg_per_user = cacheDF.withColumn('hour',hour(cacheDF.created_at))
agg_per_user.select("*")\
    .groupBy('year', 'month', 'day', 'hour', 'actor_login')\
    .agg({'event_type': 'count', 
          'push_id':'count', 
          'pull_request':'count', 
          'ref': 'mean', 
          'ref_type': 'mean', 
          'size': 'count'})\
    .sort('actor_login')\
    .show()


# %%
# WRITE AGGREGATION #2 TO DELTA GOLD PARTITION BY YEAR MONTH DAY HOUR
agg_per_user.write\
    .partitionBy('year', 'month', 'day', 'hour')\
    .mode('overwrite')\
    .format('delta')\
    .save('dbfs:/mnt/gharchive-gold/trainee=JeffreyLinn/agg_per_user/')

# %%
# 3) BREAKDOWN OF NUMBER OF COMMITS PER PUSH EVENT
commits_per_push = cacheDF.withColumn('hour',hour(cacheDF.created_at))
commits_per_push.select('year', 'month', 'day', 'hour', 'event_type', 'size', 'unique_id')\
    .where(col('event_type')\
           .isin("PushEvent"))\
    .groupBy('year', 'month', 'day', 'hour', 'unique_id', 'event_type', 'size')\
    .count()\
    .withColumnRenamed("size", "num_commits")\
    .sort(desc('num_commits'))\
    .show()


# %%
# WRITE AGGREGATION #3 TO DELTA GOLD PARTITION BY YEAR MONTH DAY HOUR
commits_per_push.write\
    .partitionBy('year', 'month', 'day', 'hour')\
    .mode("overwrite")\
    .format('delta')\
    .save('dbfs:/mnt/gharchive-gold/trainee=JeffreyLinn/commits_per_push/')


# %%
# 4) CHALLENGE: Based on the commit messages – breakdown the events by language

# ADD A NEW COLUMN FOR LANGUAGE TYPE
langDF = cacheDF.withColumn('hour',hour(cacheDF.created_at))\
                .withColumn('language', lit(""))

# SELECT THE COLUMNS AND EXPLODE THE COMMITS MESSAGE COLUMN ALIS "MESSAGE"
langDF = langDF.select('year', 'month', 'day', 'hour', 'unique_id', 'event_type', explode(langDF.commits_message).alias("message"), 'language')

# CAST THE MESSAGE COLUMN TO STRINGS
langDf = langDF.withColumn('message', langDF["message"].cast("string"))

langDF.show()

# %%
from langdetect import detect

# LOOP DF AND DETECT LANGUAGE FROM MESSAGE COLUMN
for i in langDF.collect():
    try:
        lang = detect(i['message'])
        print(lang) #testing functionality
    except:
        print("error")
        continue


# %%
# SHOW THE PARTITIONED AGGREGATED FILES IN THE S3 GOLD ARCHIVE
display(dbutils.fs.ls('dbfs:/mnt/gharchive-gold/trainee=JeffreyLinn/'))


# %%
cacheDF = cacheDF.unpersist()


