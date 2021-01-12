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

def apiFootballRequest():

    s3_client = boto3.client('s3')
    bucket = "XXX"    # specify your bucket's name in place of "XXX"

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

schedule.every().tuesday.at("08:00").do(apiFootballRequest)

while True:
    schedule.run_pending()
    time.sleep(1)
