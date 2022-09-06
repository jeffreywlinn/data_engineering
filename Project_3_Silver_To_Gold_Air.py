# %%
pip install boto

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

spark._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
spark.conf.set("spark.databricks.io.directoryCommit.createSuccessFile","false")
spark.conf.set("parquet.enable.summary-metadata", "false")
spark.conf.set("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")

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

for key in bucket2.list(prefix='aq-data-stream'):
    dt = key.last_modified
    if dt > last:
        name_list.append(key.name)

if not name_list:
    print('No new files')

else:  
    df = spark.read.parquet(SILVER_MOUNT_NAME + '/aq-data-stream/', modifiedAfter=last)

    daily = Window.partitionBy('location', 'utc_day')
    monthly = Window.partitionBy('location', 'utc_month')
    yearly = Window.partitionBy('location', 'utc_year')

    df = df.filter(col('utc_year') == 2022)\
            .filter((col('utc_month') == 8) | (col('utc_month') == 9))\
            .dropna(subset=['longitude', 'latitude'])\
            .withColumn('latitude', round(col('latitude'), 4))\
            .withColumn('longitude', round(col('longitude'), 4))\
            .withColumn('time_id', concat_ws('-', 'utc_year', 'utc_month', 'utc_day', 'utc_hour'))\
            .withColumn('location_id', concat_ws(', ', 'latitude', 'longitude'))\
            .withColumnRenamed('bc ppm', 'bc_ppm')\
            .withColumnRenamed('co ppm', 'co_ppm')\
            .withColumnRenamed('no2 ppm', 'no2_ppm')\
            .withColumnRenamed('o3 ppm', 'o3_ppm')\
            .withColumnRenamed('pm10 ppm', 'pm10_ppm')\
            .withColumnRenamed('pm25 ppm', 'pm25_ppm')\
            .withColumnRenamed('so2 ppm', 'so2_ppm')\
            .withColumn('bc_ppm_daily', avg('bc_ppm').over(daily))\
            .withColumn('co_ppm_daily', avg('co_ppm').over(daily))\
            .withColumn('no2_ppm_daily', avg('no2_ppm').over(daily))\
            .withColumn('o3_ppm_daily', avg('o3_ppm').over(daily))\
            .withColumn('pm10_ppm_daily', avg('pm10_ppm').over(daily))\
            .withColumn('pm25_ppm_daily', avg('pm25_ppm').over(daily))\
            .withColumn('so2_ppm_daily', avg('so2_ppm').over(daily))\
            .withColumn('bc_ppm_monthly', avg('bc_ppm').over(monthly))\
            .withColumn('co_ppm_monthly', avg('co_ppm').over(monthly))\
            .withColumn('no2_ppm_monthly', avg('no2_ppm').over(monthly))\
            .withColumn('o3_ppm_monthly', avg('o3_ppm').over(monthly))\
            .withColumn('pm10_ppm_monthly', avg('pm10_ppm').over(monthly))\
            .withColumn('pm25_ppm_monthly', avg('pm25_ppm').over(monthly))\
            .withColumn('so2_ppm_monthly', avg('so2_ppm').over(monthly))\
            .withColumn('bc_ppm_yearly', avg('bc_ppm').over(yearly))\
            .withColumn('co_ppm_yearly', avg('co_ppm').over(yearly))\
            .withColumn('no2_ppm_yearly', avg('no2_ppm').over(yearly))\
            .withColumn('o3_ppm_yearly', avg('o3_ppm').over(yearly))\
            .withColumn('pm10_ppm_yearly', avg('pm10_ppm').over(yearly))\
            .withColumn('pm25_ppm_yearly', avg('pm25_ppm').over(yearly))\
            .withColumn('so2_ppm_yearly', avg('so2_ppm').over(yearly))\
            .withColumn('utc_year_2', col('utc_year'))\
            .distinct()\
            .sort('city')
    
    locationdf = df.select('location_id',
                   'latitude',
                   'longitude',
                   'location',
                   'city',
                   'country')\
                   .groupBy('location_id',
                   'latitude',
                   'longitude',
                   'location',
                   'city',
                   'country')\
                   .count()\
                   .drop('count')\
                   .sort('location')

    locationdf.write.mode('append').format('parquet').save(GOLD_MOUNT_NAME + '/location/')
    
    timedf = df.select('time_id',
                   'utc_hour',
                   'utc_day',
                   'utc_month',
                   'utc_year',
                   'utc_year_2')\
                   .groupBy('time_id',
                   'utc_year',
                   'utc_year_2',
                   'utc_month',
                   'utc_day',
                   'utc_hour')\
                   .count()\
                   .drop('count')\
                   .sort('utc_year', 'utc_month', 'utc_day', 'utc_hour')

    timedf.write.partitionBy('utc_year_2').mode('append').format('parquet').save(GOLD_MOUNT_NAME + '/time/')
    
    factdf = df.select('time_id',
                    'utc_year',
                    'location_id',
                    'bc_ppm',
                    'co_ppm',
                    'no2_ppm',
                    'o3_ppm',
                    'pm10_ppm',
                    'pm25_ppm',
                    'so2_ppm',
                    'bc_ppm_daily',
                    'co_ppm_daily',
                    'no2_ppm_daily',
                    'o3_ppm_daily',
                    'pm10_ppm_daily',
                    'pm25_ppm_daily',
                    'so2_ppm_daily',
                    'bc_ppm_monthly',
                    'co_ppm_monthly',
                    'no2_ppm_monthly',
                    'o3_ppm_monthly',
                    'pm10_ppm_monthly',
                    'pm25_ppm_monthly',
                    'so2_ppm_monthly',
                    'bc_ppm_yearly',
                    'co_ppm_yearly',
                    'no2_ppm_yearly',
                    'o3_ppm_yearly',
                    'pm10_ppm_yearly',
                    'pm25_ppm_yearly',
                    'so2_ppm_yearly')

    factdf.write.partitionBy('utc_year').mode('append').format('parquet').save(GOLD_MOUNT_NAME + '/fact/')
    
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
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
spark.conf.set("spark.databricks.io.directoryCommit.createSuccessFile","false")
spark.conf.set("parquet.enable.summary-metadata", "false")
spark.conf.set("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")

df = spark.read.parquet(SILVER_MOUNT_NAME + '/aq-data-stream/')

daily = Window.partitionBy('location', 'utc_day')
monthly = Window.partitionBy('location', 'utc_month')
yearly = Window.partitionBy('location', 'utc_year')

df = df.filter(col('utc_year') == 2022)\
            .filter((col('utc_month') == 8) | (col('utc_month') == 9))\
            .dropna(subset=['longitude', 'latitude'])\
            .withColumn('latitude', round(col('latitude'), 4))\
            .withColumn('longitude', round(col('longitude'), 4))\
            .withColumn('time_id', concat_ws('-', 'utc_year', 'utc_month', 'utc_day', 'utc_hour'))\
            .withColumn('location_id', concat_ws(', ', 'latitude', 'longitude'))\
            .withColumnRenamed('bc ppm', 'bc_ppm')\
            .withColumnRenamed('co ppm', 'co_ppm')\
            .withColumnRenamed('no2 ppm', 'no2_ppm')\
            .withColumnRenamed('o3 ppm', 'o3_ppm')\
            .withColumnRenamed('pm10 ppm', 'pm10_ppm')\
            .withColumnRenamed('pm25 ppm', 'pm25_ppm')\
            .withColumnRenamed('so2 ppm', 'so2_ppm')\
            .withColumn('bc_ppm_daily', avg('bc_ppm').over(daily))\
            .withColumn('co_ppm_daily', avg('co_ppm').over(daily))\
            .withColumn('no2_ppm_daily', avg('no2_ppm').over(daily))\
            .withColumn('o3_ppm_daily', avg('o3_ppm').over(daily))\
            .withColumn('pm10_ppm_daily', avg('pm10_ppm').over(daily))\
            .withColumn('pm25_ppm_daily', avg('pm25_ppm').over(daily))\
            .withColumn('so2_ppm_daily', avg('so2_ppm').over(daily))\
            .withColumn('bc_ppm_monthly', avg('bc_ppm').over(monthly))\
            .withColumn('co_ppm_monthly', avg('co_ppm').over(monthly))\
            .withColumn('no2_ppm_monthly', avg('no2_ppm').over(monthly))\
            .withColumn('o3_ppm_monthly', avg('o3_ppm').over(monthly))\
            .withColumn('pm10_ppm_monthly', avg('pm10_ppm').over(monthly))\
            .withColumn('pm25_ppm_monthly', avg('pm25_ppm').over(monthly))\
            .withColumn('so2_ppm_monthly', avg('so2_ppm').over(monthly))\
            .withColumn('bc_ppm_yearly', avg('bc_ppm').over(yearly))\
            .withColumn('co_ppm_yearly', avg('co_ppm').over(yearly))\
            .withColumn('no2_ppm_yearly', avg('no2_ppm').over(yearly))\
            .withColumn('o3_ppm_yearly', avg('o3_ppm').over(yearly))\
            .withColumn('pm10_ppm_yearly', avg('pm10_ppm').over(yearly))\
            .withColumn('pm25_ppm_yearly', avg('pm25_ppm').over(yearly))\
            .withColumn('so2_ppm_yearly', avg('so2_ppm').over(yearly))\
            .withColumn('utc_year_2', col('utc_year'))\
            .distinct()\
            .sort('city')
    
factdf = df.select('time_id',
                    'utc_year',
                    'location_id',
                    'bc_ppm',
                    'co_ppm',
                    'no2_ppm',
                    'o3_ppm',
                    'pm10_ppm',
                    'pm25_ppm',
                    'so2_ppm',
                    'bc_ppm_daily',
                    'co_ppm_daily',
                    'no2_ppm_daily',
                    'o3_ppm_daily',
                    'pm10_ppm_daily',
                    'pm25_ppm_daily',
                    'so2_ppm_daily',
                    'bc_ppm_monthly',
                    'co_ppm_monthly',
                    'no2_ppm_monthly',
                    'o3_ppm_monthly',
                    'pm10_ppm_monthly',
                    'pm25_ppm_monthly',
                    'so2_ppm_monthly',
                    'bc_ppm_yearly',
                    'co_ppm_yearly',
                    'no2_ppm_yearly',
                    'o3_ppm_yearly',
                    'pm10_ppm_yearly',
                    'pm25_ppm_yearly',
                    'so2_ppm_yearly')

factdf.write.partitionBy('utc_year').mode('append').format('parquet').save(GOLD_MOUNT_NAME + '/fact/')

# %%



