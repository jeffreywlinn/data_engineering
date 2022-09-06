# %%
# Import the urllib and os tools
import urllib
import os

# Assign AWS Access and Secret Key to Variable to hide them from code
ACCESS_KEY = os.environ.get('ACCESS_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')

# Encodes the AWS Secret Key so it is no visible in the URL
ENCODED_SECRET_KEY = urllib.parse.quote(SECRET_KEY)

# Set AWS Bucket Name to a variable
AWS_S3_SILVER_BUCKET = 'team-2-skillstorm-silver'
# Sets AWS Silver Mount Name to a variable
SILVER_MOUNT_NAME = '/mnt/team-2-skillstorm-silver'
# Defines the Source URL needed to connect to the Silver Layer of the S3 bucket
SILVER_SOURCE_URL = f"s3n://{ACCESS_KEY}:{ENCODED_SECRET_KEY}@{AWS_S3_SILVER_BUCKET}"
#Connect to Silver Layer of the S3 Bucket
try:
    dbutils.fs.mount(SILVER_SOURCE_URL, SILVER_MOUNT_NAME)
except:
    print("Already Mounted")

# %%
# Set AWS Bucket Name to a variable
AWS_S3_GOLD_BUCKET = 'team-2-skillstorm-gold'
# Sets AWS Gold Mount Name to a variable
GOLD_MOUNT_NAME = '/mnt/team-2-skillstorm-gold'
# Defines the Source URL needed to connect to the Gold Layer of the S3 bucket
GOLD_SOURCE_URL = f"s3n://{ACCESS_KEY}:{ENCODED_SECRET_KEY}@{AWS_S3_GOLD_BUCKET}"
#Connect to Gold Layer of the S3 Bucket
try:
    dbutils.fs.mount(GOLD_SOURCE_URL, GOLD_MOUNT_NAME)
except:
    print("Already Mounted")

# %%
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import boto
from datetime import datetime

s3 = boto.connect_s3(ACCESS_KEY, SECRET_KEY)

bucket = s3.lookup('team-2-skillstorm-gold')

last = '2012-08-30T00:00:00.000Z'

for key in bucket:
    dt = key.last_modified
    if dt > last:
        last = dt

last = last.replace(".000Z", "")
        
bucket2 = s3.lookup('team-2-skillstorm-silver')

name_list = []
final_list = []

for key in bucket2.list(prefix='outside-data/weather'):
    dt = key.last_modified
    if dt > last:
        name_list.append(key.name)

if not name_list:
    print('No new files')

else:  
    df = spark.read.format('delta').load(SILVER_MOUNT_NAME + '/outside-data/weather/', modifiedAfter=last)

    df = df.withColumnRenamed('lon', 'longitude')\
                   .withColumnRenamed('lat', 'latitude')\
                   .withColumnRenamed('temp', 'temperature')\
                   .withColumnRenamed('sea_level', 'above_sea_level')\
                   .withColumnRenamed('grnd_level', 'ground_level')\
                   .distinct()

    weatherdf = df.dropna(subset=['longitude', 'latitude'])\
                    .select('longitude',
                    'latitude',
                    'temperature',
                    'feels_like',
                    'humidity',
                    'above_sea_level',
                    'wind_speed',
                    'wind_gust',
                    'datetime')\
                    .withColumn('latitude', round(col('latitude'), 4))\
                    .withColumn('longitude', round(col('longitude'), 4))\
                    .withColumn('datetime', from_unixtime('datetime'))\
                    .withColumn('year', year('datetime'))\
                    .withColumn('month', month('datetime'))\
                    .withColumn('day', dayofmonth('datetime'))\
                    .withColumn('hour', hour('datetime'))\
                    .withColumn('time_id', concat_ws('-', 'year', 'month', 'day', 'hour'))\
                    .withColumn('location_id', concat_ws(', ', 'latitude', 'longitude'))\
                    .withColumn('time_id_2', col('time_id'))\
                    .drop('datetime', 'year', 'month', 'day', 'hour', 'longitude', 'latitude')

    weatherdf.write.partitionBy('time_id_2').mode('append').format('parquet').save(GOLD_MOUNT_NAME + '/weather/')

# %%



