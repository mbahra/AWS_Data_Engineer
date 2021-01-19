import requests
import datetime
import json
import schedule
import time
import logging
import boto3
from botocore.exceptions import ClientError
import uuid

def statisticsRequest(fixtureId):

    url = "https://api-football-beta.p.rapidapi.com/fixtures/statistics"

    headers = {
    'x-rapidapi-key': "XXX", # Replace XXX by your API key
    'x-rapidapi-host': "api-football-beta.p.rapidapi.com"
    }

    querystring = {"fixture":fixtureId}

    response = requests.request("GET", url, headers=headers, params=querystring)

    return response.json()

previousWeekDate = (datetime.datetime.today() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
yesterdayDate = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
todayDate = datetime.datetime.today().strftime('%Y-%m-%d')
nextWeekDate = (datetime.datetime.today() + datetime.timedelta(days=6)).strftime('%Y-%m-%d')

data = statisticsRequest("592251")

with open('statiscsJson.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=4)
