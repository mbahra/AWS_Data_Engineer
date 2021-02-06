import boto3
import json
import urllib.parse
import datetime
import uuid
import io
import time
import pandas as pd
import requests

s3_client = boto3.client('s3')
apiKey = 'XXX'  # Replace XXX by your API key


def lambda_handler(event, context):

    def statisticsRequest(idFixture):
        """
        Sends a request to API Football Beta to get the statistics for a specified fixture.
        Returns the response as a json object.
        """

        url = "https://api-football-beta.p.rapidapi.com/fixtures/statistics"
        headers = {
            'x-rapidapi-key': apiKey,
            'x-rapidapi-host': "api-football-beta.p.rapidapi.com"
        }
        querystring = {"fixture": idFixture}
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

    # Each time that a fixtures json object is uploaded to the S3 datalake:
    # Get statistics of all finished fixtures in the json object
    # Upload the json objects as json files to the datalake into the 'raw-data' folder
    # Processed and upload the json objects as a csv file to the datalake into the 'processed-data' folder


    # Get the data lake bucket name
    dataLakeBucketName = event['Records'][0]['s3']['bucket']['name']
    # Get the file/key name
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    # Fetch the file from S3
    response = s3_client.get_object(Bucket=dataLakeBucketName, Key=key)
    # Deserialize the file's content
    text = response["Body"].read().decode()
    fixturesJson = json.loads(text)

    # Create a dataframe to prepare statistics data ingestion in csv
    df = pd.DataFrame(columns=['idFixture', 'idHomeTeam', 'idAwayTeam',
                               'shotsOnGoalHomeTeam', 'shotsOnGoalAwayTeam',
                               'shotsInsideBoxHomeTeam', 'shotsInsideBoxAwayTeam',
                               'totalShotsHomeTeam', 'totalShotsAwayTeam',
                               'ballPossessionHomeTeam', 'ballPossessionAwayTeam'])
    # Get statistics for each finished fixtures from API Football Beta
    for fixture in fixturesJson['response']:
        status = fixture['fixture']['status']['long']
        if status != 'Match Finished':
            continue
        idFixture = fixture['fixture']['id']
        statisticsJson = statisticsRequest(idFixture)
    # Upload statistics json object into 'raw-data' folder
        prefix = 'raw-data/api-football/statistics/'
        name = ''.join(['statistics-', str(idFixture), '.json'])
        uploadJsonToS3(statisticsJson, dataLakeBucketName, s3_client, prefix, name)
    # Process each statistics json data to a new dataframe row
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
        row = {'idFixture': idFixture, 'idHomeTeam': idHomeTeam, 'idAwayTeam': idAwayTeam,
               'shotsOnGoalHomeTeam': shotsOnGoalHomeTeam, 'shotsOnGoalAwayTeam': shotsOnGoalAwayTeam,
               'shotsInsideBoxHomeTeam': shotsInsideBoxHomeTeam, 'shotsInsideBoxAwayTeam': shotsInsideBoxAwayTeam,
               'totalShotsHomeTeam': totalShotsHomeTeam, 'totalShotsAwayTeam': totalShotsAwayTeam,
               'ballPossessionHomeTeam': ballPossessionHomeTeam, 'ballPossessionAwayTeam': ballPossessionAwayTeam}
        df = df.append(row, ignore_index=True)
    # Sleep 2,1 seconds between each statistics request to avoid the 30 requests / minute API Football Beta limitation
        time.sleep(2.1)
    # Upload statistics as a csv file into 'processed-data' folder
    prefix = 'processed-data/api-football/statistics/'
    todayDate = datetime.datetime.today().strftime('%Y-%m-%d')
    name = ''.join(['statistics-', todayDate, '.csv'])
    uploadCsvToS3(df, dataLakeBucketName, s3_client, prefix, name)

    print('Statistics from API-Football-Beta imported in json and csv successfully!')
