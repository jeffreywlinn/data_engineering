# %%
# PROJECT 3, LAMBDA FUNCTION OPEN WEATHER 2


from datetime import datetime
from io import StringIO  # python3; python2: BytesIO
from requests import get
from pandas import DataFrame, json_normalize, read_csv, concat
from boto3 import client, resource
import os
import time
def lambda_handler(event, context):
    key = "82b3668a3395412c9936315eb7ee7a10"
    index = 2
    date = datetime.now()
    year = date.year
    month = date.month
    day = date.day
    hour = date.hour

    AWS_ACCESS = os.environ['AWS_ACCESS']
    AWS_SECRET = os.environ['AWS_SECRET']
    s3 = client("s3",region_name='us-east-1',
                      aws_access_key_id=AWS_ACCESS,
                      aws_secret_access_key=AWS_SECRET)
    bucket = 'team-2-skillstorm-bronze'
    def get_weather(lat='35',lon='134',location=''):
        if location=='':
            url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={key}&units=metric'
            r = get(url).json()
            df = json_normalize(r)
        else:
            url = f'https://api.openweathermap.org/data/2.5/weather?q={location}&appid={key}&units=metric'
            r = get(url).json()
            df = json_normalize(r)
        return(df)
    
    def get_city_coords(city, state='',country='US'):
        url = f'http://api.openweathermap.org/geo/1.0/direct?q={city}&limit=5&appid={key}'
        r = get(url).json()
        if country=='US':
            for result in r:
                try:
                    if result['state']==state:
                        return([result['lat'],result['lon']])
                except:
                    print(f'Could not find {city} in {state}')
        else:
            for result in r:
                try:
                    if result['country']==country:
                        return([result['lat'],result['lon']])
                except:
                    print(f'could not find {city} in {country}')
    def Create_Weather_DF(out_date=0,coordinate_array=[[41.4996562,-81.6936813],get_city_coords(city='Chicago',state='Illinois')]):
        if out_date==0:
            date = datetime.now()
            year = date.year
            month = date.month
            day = date.day
            hour = date.hour
            weather_df = DataFrame(columns=['weather', 'base', 'visibility', 'dt', 'timezone', 'id', 'name', 'cod',
           'coord.lon', 'coord.lat', 'main.temp', 'main.feels_like',
           'main.temp_min', 'main.temp_max', 'main.pressure', 'main.humidity',
           'wind.speed', 'wind.deg', 'wind.gust', 'clouds.all', 'sys.type',
           'sys.id', 'sys.country', 'sys.sunrise', 'sys.sunset'])
            #count = 0
            for coords in coordinate_array:
                # count+=1
                recent_weather = get_weather(lat=str(coords[0]),lon=str(coords[1]))
                time.sleep(.5)
                weather_df = concat([weather_df,recent_weather],ignore_index=True)
                #if count == 60:
                #    break
        file_path = f'outside-weather-data/open_weather_{year}_{month}_{day}_{hour}_{index}.csv'
        #weather_df.to_csv(file_path)
        csv_buffer = StringIO()
        weather_df.to_csv(csv_buffer)
        s3_resource = resource('s3')
        s3_resource.Object(bucket, f'{file_path}').put(Body=csv_buffer.getvalue())
    def load_coordinates(index):
        obj = s3.get_object(Bucket=bucket, Key=f'coordinates_{index}.csv')
        coordinates = read_csv(obj['Body'])  # 'Body' is a key word
        out_coordinates = [[row[2],row[3]] for row in coordinates.values]
        return(out_coordinates)
    
    coords = load_coordinates(index)
    Create_Weather_DF(coordinate_array=coords)


# %%
# PROJECT 3 LAMBDA OPEN WEATHER 3

from datetime import datetime
from io import StringIO  # python3; python2: BytesIO
from requests import get
from pandas import DataFrame, json_normalize, read_csv, concat
from boto3 import client, resource
import os
import time
def lambda_handler(event, context):
    key = "c1f279a7c83eb00b23a5f2d493aac408"
    index = 3
    date = datetime.now()
    year = date.year
    month = date.month
    day = date.day
    hour = date.hour
    
    AWS_ACCESS = os.environ['AWS_ACCESS']
    AWS_SECRET = os.environ['AWS_SECRET']
    s3 = client("s3",region_name='us-east-1',
                      aws_access_key_id=AWS_ACCESS,
                      aws_secret_access_key=AWS_SECRET)
    bucket = 'team-2-skillstorm-bronze'
    def get_weather(lat='35',lon='134',location=''):
        if location=='':
            url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={key}&units=metric'
            r = get(url).json()
            df = json_normalize(r)
        else:
            url = f'https://api.openweathermap.org/data/2.5/weather?q={location}&appid={key}&units=metric'
            r = get(url).json()
            df = json_normalize(r)
        return(df)
    
    def get_city_coords(city, state='',country='US'):
        url = f'http://api.openweathermap.org/geo/1.0/direct?q={city}&limit=5&appid={key}'
        r = get(url).json()
        if country=='US':
            for result in r:
                try:
                    if result['state']==state:
                        return([result['lat'],result['lon']])
                except:
                    print(f'Could not find {city} in {state}')
        else:
            for result in r:
                try:
                    if result['country']==country:
                        return([result['lat'],result['lon']])
                except:
                    print(f'could not find {city} in {country}')
    def Create_Weather_DF(back_date=0,coordinate_array=[[41.4996562,-81.6936813],get_city_coords(city='Chicago',state='Illinois')]):
        date = datetime.now()
        year = date.year
        month = date.month
        day = date.day
        hour = date.hour
        if back_date==0:
            weather_df = DataFrame(columns=['weather', 'base', 'visibility', 'dt', 'timezone', 'id', 'name', 'cod',
           'coord.lon', 'coord.lat', 'main.temp', 'main.feels_like',
           'main.temp_min', 'main.temp_max', 'main.pressure', 'main.humidity',
           'wind.speed', 'wind.deg', 'wind.gust', 'clouds.all', 'sys.type',
           'sys.id', 'sys.country', 'sys.sunrise', 'sys.sunset'])
            #count = 0
            for coords in coordinate_array:
                #count+=1
                recent_weather = get_weather(lat=str(coords[0]),lon=str(coords[1]))
                time.sleep(.5)
                weather_df = concat([weather_df,recent_weather],ignore_index=True)
                #if count == 10:
                #    break
        file_path = f'outside-weather-data/open_weather_{year}_{month}_{day}_{hour}_{index}.csv'
        #weather_df.to_csv(file_path)
        csv_buffer = StringIO()
        weather_df.to_csv(csv_buffer)
        s3_resource = resource('s3')
        s3_resource.Object(bucket, f'{file_path}').put(Body=csv_buffer.getvalue())
    def load_coordinates(index):
        obj = s3.get_object(Bucket=bucket, Key=f'coordinates_{index}.csv')
        coordinates = read_csv(obj['Body'])  # 'Body' is a key word
        out_coordinates = [[row[2],row[3]] for row in coordinates.values]
        return(out_coordinates)
    
    coords = load_coordinates(index)
    Create_Weather_DF(coordinate_array=coords)


# %%
# PROJECT 3 LAMBDA OPEN WEATHER 4

from datetime import datetime
from io import StringIO  # python3; python2: BytesIO
from requests import get
from pandas import DataFrame, json_normalize, read_csv, concat
from boto3 import client, resource
import os
import time
def lambda_handler(event, context):
    key = "df6fa9ad07d1e6c1255e16e6f5a3da57"
    index = 4
    date = datetime.now()
    year = date.year
    month = date.month
    day = date.day
    hour = date.hour
    
    AWS_ACCESS = os.environ['AWS_ACCESS']
    AWS_SECRET = os.environ['AWS_SECRET']
    s3 = client("s3",region_name='us-east-1',
                      aws_access_key_id=AWS_ACCESS,
                      aws_secret_access_key=AWS_SECRET)
    bucket = 'team-2-skillstorm-bronze'
    def get_weather(lat='35',lon='134',location=''):
        if location=='':
            url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={key}&units=metric'
            r = get(url).json()
            df = json_normalize(r)
        else:
            url = f'https://api.openweathermap.org/data/2.5/weather?q={location}&appid={key}&units=metric'
            r = get(url).json()
            df = json_normalize(r)
        return(df)
    
    def get_city_coords(city, state='',country='US'):
        url = f'http://api.openweathermap.org/geo/1.0/direct?q={city}&limit=5&appid={key}'
        r = get(url).json()
        if country=='US':
            for result in r:
                try:
                    if result['state']==state:
                        return([result['lat'],result['lon']])
                except:
                    print(f'Could not find {city} in {state}')
        else:
            for result in r:
                try:
                    if result['country']==country:
                        return([result['lat'],result['lon']])
                except:
                    print(f'could not find {city} in {country}')
    def Create_Weather_DF(back_date=0,coordinate_array=[[41.4996562,-81.6936813],get_city_coords(city='Chicago',state='Illinois')]):
        date = datetime.now()
        year = date.year
        month = date.month
        day = date.day
        hour = date.hour
        if back_date==0:
            weather_df = DataFrame(columns=['weather', 'base', 'visibility', 'dt', 'timezone', 'id', 'name', 'cod',
           'coord.lon', 'coord.lat', 'main.temp', 'main.feels_like',
           'main.temp_min', 'main.temp_max', 'main.pressure', 'main.humidity',
           'wind.speed', 'wind.deg', 'wind.gust', 'clouds.all', 'sys.type',
           'sys.id', 'sys.country', 'sys.sunrise', 'sys.sunset'])
            #count = 0
            for coords in coordinate_array:
                #count+=1
                recent_weather = get_weather(lat=str(coords[0]),lon=str(coords[1]))
                time.sleep(.5)
                weather_df = concat([weather_df,recent_weather],ignore_index=True)
                #if count == 10:
                #    break
        file_path = f'outside-weather-data/open_weather_{year}_{month}_{day}_{hour}_{index}.csv'
        #weather_df.to_csv(file_path)
        csv_buffer = StringIO()
        weather_df.to_csv(csv_buffer)
        s3_resource = resource('s3')
        s3_resource.Object(bucket, f'{file_path}').put(Body=csv_buffer.getvalue())
    def load_coordinates(index):
        obj = s3.get_object(Bucket=bucket, Key=f'coordinates_{index}.csv')
        coordinates = read_csv(obj['Body'])  # 'Body' is a key word
        out_coordinates = [[row[2],row[3]] for row in coordinates.values]
        return(out_coordinates)
    
    coords = load_coordinates(index)
    Create_Weather_DF(coordinate_array=coords)


# %%
# PROJECT 3 LAMBDA OPEN AQ STREAM

from __future__ import print_function
import json
import boto3
from datetime import datetime as dt


# print('Loading function')


def lambda_handler(event, context):
    # print("Received event: " + json.dumps(event, indent=2))
    encoded_string = json.dumps(event, indent=3).encode('utf-8')
    message = event['Records'][0]['Sns']['Message']
    message_json = json.loads(message)
    key1 = message_json['Records'][0]['s3']['object']['key']
    bucket1 = message_json['Records'][0]['s3']['bucket']['name']
    bucket2 = 'team-2-skillstorm-bronze'
    key2 = f'openaqstream/{key1.split("/")[-1]}'
    
    s3 = boto3.resource('s3')
    copy_source={
        'Bucket': bucket1,
        'Key': key1
    }
    s3.meta.client.copy(copy_source, bucket2, key2)
    

# %%
# PROJECT 3 LAMBDA SILVER TO GOLD CLEANING

import json
import requests
import urllib.parse
import boto3

print('Loading function')

s3 = boto3.client('s3')


def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print("CONTENT TYPE: " + response['ContentType'])
        return response['ContentType']
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

job_payload = {
  "run_name": 'just_a_run',
  "existing_cluster_id": '0826-130612-hx778dvk',
  "notebook_task": 
    {
      "notebook_path": '/Users/chaseamccoy@gmail.com/Project_3_testbed'
    }
}

resp = requests.post('https://databricks.cloud.company.com/api/2.0/jobs/runs/submit', json=job_payload, headers={'Authorization': 'Bearer token'})

print(resp.status_code)

print(resp.text)

# %%
# PROJECT 3 CONTROL TOWER SNS NOTIFICATION FORWARDER

from __future__ import print_function
import boto3
import json
import os
def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    sns = boto3.client('sns')
    subject=event['Records'][0]['Sns']['Subject']
    if subject is None:
        subject = 'None'
    message = event['Records'][0]['Sns']['Message']
    try:
        msg = json.loads(message)
        message = json.dumps(msg, indent=4)
        if 'detail-type' in msg:
          subject = msg['detail-type']
    except:
        print('Not json')
    response = sns.publish(
        TopicArn=os.environ.get('sns_arn'),
        Subject=subject,
        Message=message
    )
    print(response)
    return response



