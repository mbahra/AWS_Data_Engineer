import boto3
import json
import urllib.parse
import datetime
import uuid
import io
import pandas as pd

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
            row = {'idFixture': idFixture, 'status': status, 'date': date, 'time': time,
                   'idHomeTeam': idHomeTeam, 'idAwayTeam': idAwayTeam,
                   'goalsHomeTeam': goalsHomeTeam, 'goalsAwayTeam': goalsAwayTeam}
            df = df.append(row, ignore_index=True)
        # Convert df to csv and upload it to S3 into the 'processed-data' folder
        uploadCsvToS3(df, bucket, s3Connection, prefix, name)

    # Each time that a fixtures json object is uploaded to the S3 datalake:
    # Process it and upload it as a csv file to the datalake into the 'processed-data' folder

    # Get the data lake bucket name
    dataLakeBucketName = event['Records'][0]['s3']['bucket']['name']
    # Get the file/key name
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    # Fetch the file from S3
    response = s3_client.get_object(Bucket=dataLakeBucketName, Key=key)
    # Deserialize the file's content
    text = response["Body"].read().decode()
    fixturesJson = json.loads(text)

    # Process and upload fixtures as a csv file into 'processed-data' folder
    prefix = 'processed-data/api-football/fixtures/'
    todayDate = datetime.datetime.today().strftime('%Y-%m-%d')
    name = ''.join(['fixtures-', todayDate, '.csv'])
    processAndUploadAsCsvToS3(fixturesJson, dataLakeBucketName, s3_client, prefix, name)

    print('Fixtures from API-Football-Beta processed to csv successfully!')
