import requests
import datetime
import json
import schedule
import time
import logging
import boto3
from botocore.exceptions import ClientError
import uuid

#! handle request errors
def fixturesRequest(startDate, endDate):

    url = "https://api-football-beta.p.rapidapi.com/fixtures"

    headers = {
    'x-rapidapi-key': "XXX", # Replace XXX by your API key
    'x-rapidapi-host': "api-football-beta.p.rapidapi.com"
    }

    querystring = {"league":"39", "season":"2020", "from":startDate, "to":endDate}

    response = requests.request("GET", url, headers=headers, params=querystring)

    return response.json()

#! handle request errors
def statisticsRequest(fixtureId):

    url = "https://api-football-beta.p.rapidapi.com/fixtures/statistics/fixture/" + fixtureId + "/"

    headers = {
    'x-rapidapi-key': "XXX", # Replace XXX by your API key
    'x-rapidapi-host': "api-football-beta.p.rapidapi.com"
    }

    response = requests.request("GET", url, headers=headers)

    return response.json()

def main():

    s3_client = boto3.client('s3')
    bucket = "XXX"    # Replace XXX by your S3 bucket's name

    previousWeekDate = (datetime.datetime.today() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
    yesterdayDate = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    todayDate = datetime.datetime.today().strftime('%Y-%m-%d')
    nextWeekDate = (datetime.datetime.today() + datetime.timedelta(days=6)).strftime('%Y-%m-%d')

    # Get a json file with all the next week's fixtures and put it into the "raw data" S3 bucket
    nextWeekFixturesJson = fixturesRequest(todayDate, nextWeekDate)
    data = json.dumps(nextWeekFixturesJson).encode('UTF-8')
    nextWeekFixturesJsonName = ''.join([str(uuid.uuid4().hex[:6]), "-nextWeekFixturesJson-" + todayDate])
    s3_client.put_object(Body=data, Bucket=bucket, Key=nextWeekFixturesJsonName)

    # Get a json file with all the last week's fixtures and put it into the "raw data" S3 bucket
    lastWeekFixturesJson = fixturesRequest(previousWeekDate, yesterdayDate)
    data = json.dumps(lastWeekFixturesJson).encode('UTF-8')
    lastWeekFixturesJsonName = ''.join([str(uuid.uuid4().hex[:6]), "-lastWeekFixturesJson-" + todayDate])
    s3_client.put_object(Body=data, Bucket=bucket, Key=lastWeekFixturesJsonName)

    # Get a json file of statistics per last week's fixture and put them into the "raw data" S3 bucket
    for fixture in lastWeekFixturesJson['response']:
        fixtureId = fixture['fixture']['id']
        statistiscsJson = statisticsRequest(fixtureId)
        data = json.dumps(fixturesJson).encode('UTF-8')
        statistiscsJsonName = ''.join([str(uuid.uuid4().hex[:6]), "-statiscsJson-" + str(fixtureId)])   # construction of the filename with an uuid prefix to avoid partition issue
        s3_client.put_object(Body=data, Bucket=bucket, Key=statistiscsJsonName)

if __name__ == '__main__':

    schedule.every().tuesday.at("08:00").do(main)

    while True:
        schedule.run_pending()
        time.sleep(1)
