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
        df.to_csv(csvBuffer, index=False)
        # The key has a uuid prefix to avoid partition issue
        key = ''.join([prefix, str(uuid.uuid4().hex[:6]), '-', name])
        s3Connection.put_object(Body=csvBuffer.getvalue(), Bucket=bucket, Key=key)
        print(key + ' uploaded into ' + bucket)

    def processAndUploadAsCsvToS3(data, bucket, s3Connection, prefix, name):
        """
        Converts a statistics json file to a dataframe containing the relevant data.
        Converts the dataframe to a csv.
        Uploads the csv file directly to S3 without storing it locally.
        """

        # Process the json object's data to a dataframe
        df = pd.DataFrame(columns=['idFixture', 'idHomeTeam', 'idAwayTeam',
                                    'shotsOnGoalHomeTeam', 'shotsOnGoalAwayTeam',
                                    'shotsInsideBoxHomeTeam', 'shotsInsideBoxAwayTeam',
                                    'totalShotsHomeTeam', 'totalShotsAwayTeam',
                                    'ballPossessionHomeTeam', 'ballPossessionAwayTeam'])
        idFixture = statisticsJson['parameters']['fixture']
        homeTeam = statisticsJson['response'][0]
        awayTeam = statisticsJson['response'][1]
        idHomeTeam = homeTeam['team']['id']
        idAwayTeam = awayTeam['team']['id']
        shotsOnGoalHomeTeam = homeTeam['statistics'][0]['value']
        shotsOnGoalAwayTeam = awayTeam['statistics'][0]['value']
        shotsInsideBoxHomeTeam = homeTeam['statistics'][4]['value']
        shotsInsideBoxAwayTeam = awayTeam['statistics'][4]['value']
        totalShotsHomeTeam = homeTeam['statistics'][2]['value']
        totalShotsAwayTeam = awayTeam['statistics'][2]['value']
        ballPossessionHomeTeam = homeTeam['statistics'][9]['value']
        ballPossessionAwayTeam = awayTeam['statistics'][9]['value']
        row = {'idFixture':idFixture,'idHomeTeam':idHomeTeam, 'idAwayTeam':idAwayTeam,
                'shotsOnGoalHomeTeam':shotsOnGoalHomeTeam, 'shotsOnGoalAwayTeam':shotsOnGoalAwayTeam,
                'shotsInsideBoxHomeTeam':shotsInsideBoxHomeTeam, 'shotsInsideBoxAwayTeam':shotsInsideBoxAwayTeam,
                'totalShotsHomeTeam':totalShotsHomeTeam, 'totalShotsAwayTeam':totalShotsAwayTeam,
                'ballPossessionHomeTeam':ballPossessionHomeTeam, 'ballPossessionAwayTeam':ballPossessionAwayTeam}
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
    statisticsJson = json.loads(text)

    #5 - Process and upload statistics as a csv file into 'processed-data' folder
    prefix = 'processed-data/api-football/statistics/'
    todayDate = datetime.datetime.today().strftime('%Y-%m-%d')
    name = ''.join(['statistics-', todayDate, '.csv'])
    processAndUploadAsCsvToS3(statisticsJson, dataLakeBucketName, s3_client, prefix, name)

    print('Statistics from API-Football processed to csv successfully!')
