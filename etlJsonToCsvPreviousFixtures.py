import datetime
import json
import csv
import boto3
import uuid
import io
import pandas as pd
from botocore.exceptions import ClientError
import urllib.parse

s3_client = boto3.client('s3')

def lambda_handler(event, context):

    def uploadCsvToS3(df, bucket, s3Connection, prefix, name):
        """
        Converts a dataframe to a csv,
        then uploads the csv file directly to S3 without storing it locally.
        """

        csvBuffer = io.StringIO()
        df.to_csv(csvBuffer)
        # The key has a uuid prefix to avoid partition issue
        key = ''.join([prefix, str(uuid.uuid4().hex[:6]), '-', name])
        s3Connection.put_object(Body=csvBuffer.getvalue(), Bucket=bucket, Key=key)
        print(key + ' uploaded into ' + bucket)

    def processAndUploadAsCsvToS3(data, bucket, s3Connection, prefix, name):
        """
        Converts a fixture json file to a dataframe containing the relevant data.
        Converts the dataframe to a csv.
        Uploads the csv file directly to S3 without storing it locally.
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
            row = {'idFixture':idFixture, 'status':status, 'date':date, 'time':time,
                    'idHomeTeam':idHomeTeam, 'idAwayTeam':idAwayTeam,
                    'goalsHomeTeam':goalsHomeTeam, 'goalsAwayTeam':goalsAwayTeam}
            df = df.append(row, ignore_index=True)
        # Convert df to csv and upload it to S3 into the 'processed-data' folder
        uploadCsvToS3(df, bucket, s3Connection, prefix, name)

    #1 - Get the data lake bucket name
    dataLakeBucketName = event['Records'][0]['s3']['bucket']['name']

    #2 - Get the file/key name
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    #3 - Fetch the file from S3
    response = s3_client.get_object(Bucket=dataLakeBucketName, Key=key)

    #4 - Deserialize the file's content
    text = response["Body"].read().decode()
    previousFixturesJson = json.loads(text)

    #5 - Process and upload previous week fixtures as a csv file into 'processed-data' folder
    prefix = 'processed-data/api-football/previous-fixtures/'
    todayDate = datetime.datetime.today().strftime('%Y-%m-%d')
    name = ''.join(['previousFixtures-', todayDate, '.csv'])
    processAndUploadAsCsvToS3(previousFixturesJson, dataLakeBucketName, s3_client, prefix, name)

    print('Previous fixtures from API-Football processed to csv successfully!')
