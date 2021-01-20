import requests
import datetime
import json
import csv
import schedule
import time
import logging
import boto3
from botocore.exceptions import ClientError
import uuid

def createBucketName(bucketPrefix):
    # The generated bucket name must be between 3 and 63 chars long
    # A UUID4’s string representation is 36 characters long
    # bucket_prefix must be less than 27 chars long
    return ''.join([bucketPrefix, str(uuid.uuid4())])

def createBucket(bucketPrefix, s3Connection):
    session = boto3.session.Session()
    currentRegion = session.region_name
    bucketName = createBucketName(bucketPrefix)
    bucketResponse = s3Connection.create_bucket(
        Bucket=bucketName,
        CreateBucketConfiguration={
        'LocationConstraint': currentRegion})
    print("S3 bucket created:", bucketName, currentRegion)
    return bucketResponse, bucketName

def fixturesRequest(startDate, endDate):

    url = "https://api-football-beta.p.rapidapi.com/fixtures"

    headers = {
    'x-rapidapi-key': "XXX", # Replace XXX by your API key
    'x-rapidapi-host': "api-football-beta.p.rapidapi.com"
    }

    querystring = {"league":"39", "season":"2020", "from":startDate, "to":endDate}

    response = requests.request("GET", url, headers=headers, params=querystring)

    return response.json()

def statisticsRequest(idFixture):

    url = "https://api-football-beta.p.rapidapi.com/fixtures/statistics"

    headers = {
    'x-rapidapi-key': "XXX", # Replace XXX by your API key
    'x-rapidapi-host': "api-football-beta.p.rapidapi.com"
    }

    querystring = {"fixture":idFixture}

    response = requests.request("GET", url, headers=headers, params=querystring)

    return response.json()

def main():

    s3_client = boto3.client('s3')

    ### 1/  Creation of a "DataLake" bucket

    dataLakeBucket, dataLakeBucketName = createBucket("datalake-", s3_client)

    ### 2/  Upload teamcodes.csv to the datalake without a folder named 'raw-data'

    s3_client.upload_file('teamcodes.csv', dataLakeBucketName, 'raw-data/teamcodes.csv')
    print("teamcodes.csv uploaded into " + dataLakeBucketName + "/raw-data/")

    ### 3/  Get every previous fixtures, their statistics, and the next week fixtures from API Football
    ###     Upload the json objects to the datalake without the 'raw-data' folder
    ###     Process each received json response to a dataframe
    ###     Upload each dataframe as csv file to the datalake without the 'processed-data' folder

    firstFixtureDate = '2020-09-10'
    yesterdayDate = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    todayDate = datetime.datetime.today().strftime('%Y-%m-%d')
    nextWeekDate = (datetime.datetime.today() + datetime.timedelta(days=6)).strftime('%Y-%m-%d')

    # Get next week fixtures from API Football
    nextWeekFixturesJson = fixturesRequest(todayDate, nextWeekDate)
    # Upload next week fixtures json object to 'raw-data' folder
    # Construction of every key in the script is using an uuid prefix to avoid partition issue
    data = json.dumps(nextWeekFixturesJson).encode('UTF-8')
    nextWeekFixturesJsonKey = ''.join(['raw-data/', str(uuid.uuid4().hex[:6]),
                                        "-nextWeekFixturesJson-", todayDate])
    s3_client.put_object(Body=data, Bucket=dataLakeBucketName, Key=nextWeekFixturesJsonKey)
    # Process json data to a dataframe
    df = pd.DataFrame(columns=['idFixture', 'status', 'date', 'hour', 'idHomeTeam', 'idAwayTeam'])
    for fixture in data['response']:
        idFixture = fixture['fixture']['id']
        status = fixture['fixture']['status']['long']
        date = fixture['fixture']['date'][:10]
        hour = fixture['fixture']['date'][11:16]
        idHomeTeam = fixture['teams']['home']['id']
        idAwayTeam = fixture['teams']['away']['id']
        row = {'idFixture':idFixture, 'status':status, 'date':date,
                'hour':hour, 'idHomeTeam':idHomeTeam, 'idAwayTeam':idAwayTeam}
        df = df.append(row, ignore_index=True)
    # Upload next week fixtures csv file to 'processed-data' folder
    csvBuffer = StringIO()
    df.to_csv(csvBuffer)
    nextWeekFixturesCsvKey = ''.join(['processed-data/', str(uuid.uuid4().hex[:6]),
                                        "-nextWeekFixturesCsv-", todayDate])
    s3_client.put_object(Body=csvBuffer.getvalue(), Bucket=dataLakeBucketName,
                            Key=nextWeekFixturesCsvKey)

    # Get previous fixtures from API Football
    previousFixturesJson = fixturesRequest(firstFixtureDate, yesterdayDate)
    # Upload previous fixtures json object to 'raw-data' folder
    data = json.dumps(previousFixturesJson).encode('UTF-8')
    previousFixturesJsonKey = ''.join(['raw-data/', str(uuid.uuid4().hex[:6]),
                                        "-previousFixturesJson-", todayDate])
    s3_client.put_object(Body=data, Bucket=dataLakeBucketName, Key=previousFixturesJsonKey)
    # Process json data to a dataframe
    df = pd.DataFrame(columns=['idFixture', 'status', 'date', 'hour',
                                'idHomeTeam', 'idAwayTeam', 'goalsHomeTeam', 'goalsAwayTeam'])
    for fixture in data['response']:
        idFixture = fixture['fixture']['id']
        status = fixture['fixture']['status']['long']
        date = fixture['fixture']['date'][:10]
        hour = fixture['fixture']['date'][11:16]
        idHomeTeam = fixture['teams']['home']['id']
        idAwayTeam = fixture['teams']['away']['id']
        goalsHomeTeam = fixture['goals']['home']
        goalsAwayTeam = fixture['goals']['away']
        row = {'idFixture':idFixture, 'status':status, 'date':date, 'hour':hour,
                'idHomeTeam':idHomeTeam, 'idAwayTeam':idAwayTeam,
                'goalsHomeTeam':goalsHomeTeam, 'goalsAwayTeam':goalsAwayTeam}
        df = df.append(row, ignore_index=True)
    # Upload previous fixtures csv file to 'processed-data' folder
    csvBuffer = StringIO()
    df.to_csv(csvBuffer)
    previousFixturesCsvKey = ''.join(['processed-data/', str(uuid.uuid4().hex[:6]),
                                        "-previousFixturesCsv-", todayDate])
    s3_client.put_object(Body=csvBuffer.getvalue(), Bucket=dataLakeBucketName,
                            Key=previousFixturesCsvKey)

    # Get statistics for the previous fixtures from API Football
    for fixture in previousFixturesJson['response']:
        idFixture = fixture['fixture']['id']
        statisticsJson = statisticsRequest(idFixture)
    # Upload statistics json object to 'raw-data' folder
        data = json.dumps(statisticsJson).encode('UTF-8')
        statisticsJsonKey = ''.join(['raw-data/', str(uuid.uuid4().hex[:6]),
                                        "-statisticsJson-", str(idFixture)])
        s3_client.put_object(Body=data, Bucket=dataLakeBucketName, Key=statisticsJsonKey)
    # Process json data to a dataframe
        df = pd.DataFrame(columns=['idFixture', 'idHomeTeam', 'idAwayTeam',
                                    'shotsOnGoalHomeTeam', 'shotsOnGoalAwayTeam',
                                    'shotsInsideBoxHomeTeam', 'shotsInsideBoxAwayTeam',
                                    'totalShotsHomeTeam', 'totalShotsAwayTeam',
                                    'ballPossessionHomeTeam', 'ballPossessionAwayTeam'])
        idHomeTeam = data['response'][0]['team']['id']
        idAwayTeam = data['response'][1]['team']['id']
        shotsOnGoalHomeTeam = data['response'][0]['statistics'][0]['value']
        shotsOnGoalAwayTeam = data['response'][1]['statistics'][0]['value']
        shotsInsideBoxHomeTeam = data['response'][0]['statistics'][4]['value']
        shotsInsideBoxAwayTeam = data['response'][1]['statistics'][4]['value']
        totalShotsHomeTeam = data['response'][0]['statistics'][2]['value']
        totalShotsAwayTeam = data['response'][1]['statistics'][2]['value']
        ballPossessionHomeTeam = data['response'][0]['statistics'][9]['value']
        ballPossessionAwayTeam = data['response'][1]['statistics'][9]['value']
        row = {'idFixture':idFixture,'idHomeTeam':idHomeTeam, 'idAwayTeam':idAwayTeam,
                'shotsOnGoalHomeTeam':shotsOnGoalHomeTeam, 'shotsOnGoalAwayTeam':shotsOnGoalAwayTeam,
                'shotsInsideBoxHomeTeam':shotsInsideBoxHomeTeam, 'shotsInsideBoxAwayTeam':shotsInsideBoxAwayTeam,
                'totalShotsHomeTeam':totalShotsHomeTeam, 'totalShotsAwayTeam':totalShotsAwayTeam,
                'ballPossessionHomeTeam':ballPossessionHomeTeam, 'ballPossessionAwayTeam':ballPossessionAwayTeam}
        df = df.append(row, ignore_index=True)
    # Upload statistics csv file to 'processed-data' folder
    csvBuffer = StringIO()
    df.to_csv(csvBuffer)
    statisticsCsvKey = ''.join(['processed-data/', str(uuid.uuid4().hex[:6]),
                                "-statisticsCsv-", todayDate])
    s3_client.put_object(Body=csvBuffer.getvalue(), Bucket=dataLakeBucketName, Key=statisticsCsvKey)

#! schedule requests to avoid API Football cost and limitation (100/day and 30/minute)
if __name__ == '__main__':
    main()
