# import json
#
# jsonFile = open('jsonFileTest.json', 'r')
# jsonData = jsonFile.read()
#
# fixturesJson = json.loads(jsonData)
#
# for fixture in fixturesJson['response']:  #! write with the correct json syntax
#     fixtureId = fixture['fixture']['id']
#     print(fixtureId)   #! specify your bucket's name

import requests
import datetime
import json
import schedule
import time
import logging
import boto3
from botocore.exceptions import ClientError
import uuid

todayDate = datetime.datetime.today().strftime('%Y-%m-%d')
previousWeekDate = (datetime.datetime.today() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')

#! handle request errors
def fixturesRequest():

    url = "https://api-football-beta.p.rapidapi.com/fixtures"

    headers = {
    'x-rapidapi-key': "XXX", # Write your api key in place of XXX
    'x-rapidapi-host': "api-football-beta.p.rapidapi.com"
    }

    querystring = {"league":"39", "season":"2020", "from":previousWeekDate, "to":todayDate}

    response = requests.request("GET", url, headers=headers, params=querystring)

    return response.json()

#! handle request errors
def statisticsRequest(fixtureId):

    url = "https://api-football-beta.p.rapidapi.com/fixtures/statistics/fixture/" + fixtureId + "/"

    headers = {
    'x-rapidapi-key': "XXX", # Write your api key in place of XXX
    'x-rapidapi-host': "api-football-beta.p.rapidapi.com"
    }

    response = requests.request("GET", url, headers=headers)

    return response.json()

# def upload_file(file_name, bucket, object_name=None):
#     """Upload a file to an S3 bucket
#
#     :param file_name: File to upload
#     :param bucket: Bucket to upload to
#     :param object_name: S3 object name. If not specified then file_name is used
#     :return: True if file was uploaded, else False
#     """
#
#     if object_name is None:
#         object_name = file_name
#
#     s3_client = boto3.client('s3')
#     try:
#         response = s3_client.upload_file(file_name, bucket, object_name)
#     except ClientError as e:
#         logging.error(e)
#         return False
#     return True

s3_client = boto3.client('s3')

bucket = "raw-data-d8145b7b-bac0-4f2a-a575-603def349a6b"    #! specify your bucket's name

fixturesJson = fixturesRequest()
data = json.dumps(fixturesJson).encode('UTF-8')
fixturesJsonName = ''.join([str(uuid.uuid4().hex[:6]), "-fixturesJson-" + todayDate])   #! specify that is a global variable?
s3_client.put_object(Body=data, Bucket=bucket, Key=fixturesJsonName)

for fixture in fixturesJson['response']:  #! write with the correct json syntax
    fixtureId = fixture['fixture']['id']
    statistiscsJson = statisticsRequest(fixtureId)
    data = json.dumps(fixturesJson).encode('UTF-8')
    statistiscsJsonName = ''.join([str(uuid.uuid4().hex[:6]), "-statiscsJson-" + str(fixtureId)])   # construction of the filename with an uuid prefix to avoid partition issue
    s3_client.put_object(Body=data, Bucket=bucket, Key=statistiscsJsonName)
