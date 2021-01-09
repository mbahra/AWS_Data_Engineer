import requests
import datetime
import json
import schedule
import time
import logging
import boto3
from botocore.exceptions import ClientError

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

#! add S3 connexion

#! construct the filename with an uuid prefix to avoid partition issue
def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    if object_name is None:
        object_name = file_name

    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def apiFootballRequest():

    #! connexion to S3 bucket

    fixturesJson = fixturesRequest()
    fixturesJsonName = "fixturesJson_" + todayDate
    upload_file(fixturesJson, bucket, fixturesJsonName) #! specify your bucket's name

    for fixtureId in fixturesJson:  #! write with the correct json syntax
        statistiscsJson = statisticsRequest(fixtureId)
        statistiscsJsonName = "statiscsJson_" + fixtureId
        upload_file(statistiscsJson, bucket, statistiscsJsonName)   #! specify your bucket's name

schedule.every.tuesday.at("08:00").do(apiFootballRequest)

while True:
    schedule.run_pending()
    time.sleep(1)
