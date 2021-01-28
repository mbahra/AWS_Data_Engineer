import requests
import datetime
import json
import boto3
import uuid
from botocore.exceptions import ClientError

s3_client = boto3.client('s3')
dataLakeBucketName = 'XXX'  # Replace XXX by your bucket name
apiKey = 'XXX'  # Replace XXX by your API key

def lambda_handler(event, context):

    def fixturesRequest(startDate, endDate):
        """
        Sends a request to API Football to get the 2020/2021 english Premier League fixtures
        from a starting date to an ending date, then returns the response without a json object.
        """

        url = "https://api-football-beta.p.rapidapi.com/fixtures"
        headers = {
        'x-rapidapi-key': apiKey,
        'x-rapidapi-host': "api-football-beta.p.rapidapi.com"
        }
        querystring = {"league":"39", "season":"2020", "from":startDate, "to":endDate}
        response = requests.request("GET", url, headers=headers, params=querystring)
        return response.json()

    def statisticsRequest(idFixture):
        """
        Sends a request to API Football to get the statistics for a specified fixture.
        Returns the response as a json object.
        """

        url = "https://api-football-beta.p.rapidapi.com/fixtures/statistics"
        headers = {
        'x-rapidapi-key': apiKey,
        'x-rapidapi-host': "api-football-beta.p.rapidapi.com"
        }
        querystring = {"fixture":idFixture}
        response = requests.request("GET", url, headers=headers, params=querystring)
        return response.json()

    def uploadJsonToS3(jsonObject, bucket, s3Connection, prefix, name):
        """
        Uploads json object to S3 by encoding it in utf-8.
        """

        data = json.dumps(jsonObject).encode('UTF-8')
        # The key has a uuid prefix to avoid partition issue
        key = ''.join([prefix, str(uuid.uuid4().hex[:6]), '-', name])
        s3Connection.put_object(Body=data, Bucket=bucket, Key=key)
        print(key + ' uploaded into ' + bucket)

    # Run each of these tasks weekly:
    # Get every previous week fixtures, their statistics, and the next week fixtures from API Football
    # Upload the json objects as json files to the datalake into the 'raw-data' folder

    previousWeekDate = (datetime.datetime.today() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
    yesterdayDate = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    todayDate = datetime.datetime.today().strftime('%Y-%m-%d')
    nextWeekDate = (datetime.datetime.today() + datetime.timedelta(days=6)).strftime('%Y-%m-%d')

    # NEXT WEEK FIXTURES
    # Get next week fixtures from API Football
    nextWeekFixturesJson = fixturesRequest(todayDate, nextWeekDate)
    # Upload next week fixtures json object as json file into 'raw-data' folder
    prefix = 'raw-data/api-football/next-fixtures/'
    name = ''.join(['nextWeekFixtures-', todayDate, '.json'])
    uploadJsonToS3(nextWeekFixturesJson, dataLakeBucketName, s3_client, prefix, name)

    # PREVIOUS WEEK FIXTURES
    # Get previous week fixtures from API Football
    previousFixturesJson = fixturesRequest(previousWeekDate, yesterdayDate)
    # Upload previous week fixtures json object as json file into 'raw-data' folder
    prefix = 'raw-data/api-football/previous-fixtures/'
    name = ''.join(['previousFixtures-', todayDate, '.json'])
    uploadJsonToS3(previousFixturesJson, dataLakeBucketName, s3_client, prefix, name)

    # STATISTICS
    # Get statistics for each previous fixtures from API Football
    for fixture in previousFixturesJson['response']:
        status = fixture['fixture']['status']['long']
        if status == 'Match Postponed':
            continue
        idFixture = fixture['fixture']['id']
        statisticsJson = statisticsRequest(idFixture)
    # Upload statistics json object as json file into 'raw-data' folder
        prefix = 'raw-data/api-football/statistics/'
        name = ''.join(['statistics-', str(idFixture), '.json'])
        uploadJsonToS3(statisticsJson, dataLakeBucketName, s3_client, prefix, name)

    print(todayDate, 'Lambda ETL job ApiFootballRequests performed successfully!')
