import boto3
import json
import requests
import pandas as pd
import io
import datetime
import uuid

s3_client = boto3.client('s3')
dataLakeBucketName = 'XXX'  # Replace XXX by your bucket name
apiKey = 'XXX'  # Replace XXX by your API key


def lambda_handler(event, context):

    def fixturesRequest(startDate, endDate):
        """
        Sends a request to API Football Beta to get the 2020/2021 english Premier League fixtures
        from a starting date to an ending date, then returns the response without a json object.
        """

        url = "https://api-football-beta.p.rapidapi.com/fixtures"
        headers = {
            'x-rapidapi-key': apiKey,
            'x-rapidapi-host': "api-football-beta.p.rapidapi.com"
        }
        querystring = {"league": "39", "season": "2020", "from": startDate, "to": endDate}
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

    def uploadCsvToS3(df, bucket, s3Connection, prefix, name):
        """
        Converts a dataframe to a csv,
        then uploads the csv file directly to S3.
        """

        csvBuffer = io.StringIO()
        df.to_csv(csvBuffer, index=False)
        # The key has a uuid prefix to avoid partition issue
        key = ''.join([prefix, str(uuid.uuid4().hex[:6]), '-', name])
        s3Connection.put_object(Body=csvBuffer.getvalue(), Bucket=bucket, Key=key)
        print(key + ' uploaded into ' + bucket)

    def uploadFixturesCsvToS3(data, bucket, s3Connection, prefix, name):
        """
        Converts a fixture json file to a dataframe containing the relevant data.
        Converts the dataframe to a csv.
        Uploads the csv file directly to S3.
        """

        # Process the json object's data to a dataframe
        df = pd.DataFrame(columns=['idFixture', 'status', 'date', 'time',
                                   'idHomeTeam', 'idAwayTeam', 'goalsHomeTeam', 'goalsAwayTeam'])
        for fixture in data['response']:
            idFixture = fixture['fixture']['id']
            status = fixture['fixture']['status']['long']
            date = fixture['fixture']['date'][:10]
            time = fixture['fixture']['date'][11:16]
            idHomeTeam = fixture['teams']['home']['id']
            idAwayTeam = fixture['teams']['away']['id']
            goalsHomeTeam = fixture['goals']['home']
            goalsAwayTeam = fixture['goals']['away']
            row = {'idFixture': idFixture, 'status': status, 'date': date, 'time': time,
                   'idHomeTeam': idHomeTeam, 'idAwayTeam': idAwayTeam,
                   'goalsHomeTeam': goalsHomeTeam, 'goalsAwayTeam': goalsAwayTeam}
            df = df.append(row, ignore_index=True)
        # Convert df to csv and upload it to S3 into the 'processed-data' folder
        uploadCsvToS3(df, bucket, s3Connection, prefix, name)

    # Run each of these tasks weekly:
    # Get previous and next week fixtures from API Football Beta
    # Upload the json object as a json file to the datalake into the 'raw-data' folder
    # Processed and upload the json object as a csv file to the datalake into the 'processed-data' folder

    previousWeekDate = (datetime.datetime.today() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
    todayDate = datetime.datetime.today().strftime('%Y-%m-%d')
    nextWeekDate = (datetime.datetime.today() + datetime.timedelta(days=6)).strftime('%Y-%m-%d')

    # Get fixtures from API Football Beta
    fixturesJson = fixturesRequest(previousWeekDate, nextWeekDate)
    # Upload previous week fixtures json object as json file into 'raw-data' folder
    prefix = 'raw-data/api-football/fixtures/'
    name = ''.join(['fixtures-', todayDate, '.json'])
    uploadJsonToS3(fixturesJson, dataLakeBucketName, s3_client, prefix, name)
    # Process and upload fixtures as a csv file into 'processed-data' folder
    prefix = 'processed-data/api-football/fixtures/'
    name = ''.join(['fixtures-', todayDate, '.csv'])
    uploadFixturesCsvToS3(fixturesJson, dataLakeBucketName, s3_client, prefix, name)

    print('Fixtures from API-Football-Beta imported in json and csv successfully!')
