import requests
import datetime
import json
import csv
import time
import boto3
import uuid
import io
import pandas as pd
from botocore.exceptions import ClientError

def createBucketName(bucketPrefix):
    """
    Creates and returns a globally unique bucket name from the prefix provided using a uuid4.
    The generated bucket name must be between 3 and 63 chars long.
    A uuid4’s string representation is 36 characters long so
    the prefix provided must be less than 27 chars long.
    """

    return ''.join([bucketPrefix, str(uuid.uuid4())])

def createBucket(bucketPrefix, s3Connection):
    """
    Creates an S3 bucket with a globally unique name made from the prefix provided.
    The prefix provided must be less than 27 chars long.
    Returns the boto3 response and the bucket name.
    """

    session = boto3.session.Session()
    currentRegion = session.region_name
    bucketName = createBucketName(bucketPrefix)
    bucketResponse = s3Connection.create_bucket(
        Bucket=bucketName,
        CreateBucketConfiguration={
        'LocationConstraint': currentRegion})
    print("S3 bucket created:", bucketName, currentRegion)
    return bucketName

def fixturesRequest():
    """
    Sends a request to API Football to get the 2020/2021 english Premier League fixtures
    from a starting date, then returns the response as a json object.
    """

    url = "https://api-football-v1.p.rapidapi.com/v2/fixtures/league/2790"
    querystring = {"timezone":"Europe/London"}
    headers = {
        'x-rapidapi-key': "XXX",    # Replace XXX by your API key
        'x-rapidapi-host': "api-football-v1.p.rapidapi.com"
        }
    response = requests.request("GET", url, headers=headers, params=querystring)
    return response.json()

def statisticsRequest(idFixture):
    """
    Sends a request to API Football to get the statistics for a specified fixture.
    Returns the response as a json object.
    """

    url = "https://api-football-v1.p.rapidapi.com/v2/statistics/fixture/" + str(idFixture)
    headers = {
        'x-rapidapi-key': "XXX",    # Replace XXX by your API key
        'x-rapidapi-host': "api-football-v1.p.rapidapi.com"
        }
    response = requests.request("GET", url, headers=headers)
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
    then uploads the csv file directly to S3 without storing it locally.
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
    Uploads the csv file directly to S3 without storing it locally.
    """

    # Process the json object's data to a dataframe
    df = pd.DataFrame(columns=['idFixture', 'status', 'date', 'time',
                                'idHomeTeam', 'idAwayTeam', 'goalsHomeTeam', 'goalsAwayTeam'])
    for fixture in data['api']['fixtures']:
        idFixture = fixture['fixture_id']
        status = fixture['status']
        date = fixture['event_date'][:10]
        time = fixture['event_date'][11:16]
        idHomeTeam = fixture['homeTeam']['team_id']
        idAwayTeam = fixture['awayTeam']['team_id']
        goalsHomeTeam = fixture['goalsHomeTeam']
        goalsAwayTeam = fixture['goalsAwayTeam']
        row = {'idFixture':idFixture, 'status':status, 'date':date, 'time':time,
                'idHomeTeam':idHomeTeam, 'idAwayTeam':idAwayTeam,
                'goalsHomeTeam':goalsHomeTeam, 'goalsAwayTeam':goalsAwayTeam}
        df = df.append(row, ignore_index=True)
    # Convert df to csv and upload it to S3 into the 'processed-data' folder
    uploadCsvToS3(df, bucket, s3Connection, prefix, name)

def main():

    s3_client = boto3.client('s3')

    ### 1/  Creation of the "data lake" bucket

    dataLakeBucketName = createBucket('datalake-', s3_client)

    ### 2/  Upload teamcodes.csv to the data lake into a folder named 'processed-data'

    s3_client.upload_file('teamcodes.csv', dataLakeBucketName, 'processed-data/teamcodes.csv')
    print('processed-data/teamcodes.csv uploaded into ' + dataLakeBucketName)

    ### 3/  Get every previous fixtures and their statistics from API Football
    ###     Upload the json objects to the datalake into the 'raw-data' folder
    ###     Convert the json objects to csv
    ###     Upload csv directly to the datalake into the 'processed-data' folder

    todayDate = datetime.datetime.today().strftime('%Y-%m-%d')

    # FIXTURES
    # Get fixtures from API Football
    fixturesJson = fixturesRequest()
    # Upload fixtures json object into 'raw-data' folder
    prefix = 'raw-data/api-football/fixtures/'
    name = ''.join(['fixtures-', todayDate, '.json'])
    uploadJsonToS3(fixturesJson, dataLakeBucketName, s3_client, prefix, name)
    # Process and upload fixtures as a csv file into 'processed-data' folder
    prefix = 'processed-data/api-football/fixtures/'
    name = ''.join(['fixtures-', todayDate, '.csv'])
    uploadFixturesCsvToS3(fixturesJson, dataLakeBucketName, s3_client, prefix, name)

    # STATISTICS
    # Create a dataframe to prepare statistics data ingestion in csv
    df = pd.DataFrame(columns=['idFixture', 'idHomeTeam', 'idAwayTeam',
                                'shotsOnGoalHomeTeam', 'shotsOnGoalAwayTeam',
                                'shotsInsideBoxHomeTeam', 'shotsInsideBoxAwayTeam',
                                'totalShotsHomeTeam', 'totalShotsAwayTeam',
                                'ballPossessionHomeTeam', 'ballPossessionAwayTeam'])
    # Get statistics for each finished fixtures from API Football
    # WARNING!!! API Football is free until 100 requests / day, and costs around €0.00450 / request beyond
    #           As there are 10 fixtures / round, running this script after the 9th round will incur some costs
    for fixture in fixturesJson['api']['fixtures']:
        status = fixture['status']
        if status != 'Match Finished':
            continue
        idFixture = fixture['fixture_id']
        statisticsJson = statisticsRequest(idFixture)
    # Upload statistics json object into 'raw-data' folder
        prefix = 'raw-data/api-football/statistics/'
        name = ''.join(['statistics-', str(idFixture), '.json'])
        uploadJsonToS3(statisticsJson, dataLakeBucketName, s3_client, prefix, name)
    # Process each statistics json data to a new dataframe row
        idHomeTeam = fixture['homeTeam']['team_id']
        idAwayTeam = fixture['awayTeam']['team_id']
        shotsOnGoalHomeTeam = statisticsJson['api']['statistics']['Shots on Goal']['home']
        shotsOnGoalAwayTeam = statisticsJson['api']['statistics']['Shots on Goal']['away']
        shotsInsideBoxHomeTeam = statisticsJson['api']['statistics']['Shots insidebox']['home']
        shotsInsideBoxAwayTeam = statisticsJson['api']['statistics']['Shots insidebox']['away']
        totalShotsHomeTeam = statisticsJson['api']['statistics']['Total Shots']['home']
        totalShotsAwayTeam = statisticsJson['api']['statistics']['Total Shots']['away']
        ballPossessionHomeTeam = statisticsJson['api']['statistics']['Ball Possession']['home']
        ballPossessionAwayTeam = statisticsJson['api']['statistics']['Ball Possession']['away']
        row = {'idFixture':idFixture,'idHomeTeam':idHomeTeam, 'idAwayTeam':idAwayTeam,
                'shotsOnGoalHomeTeam':shotsOnGoalHomeTeam, 'shotsOnGoalAwayTeam':shotsOnGoalAwayTeam,
                'shotsInsideBoxHomeTeam':shotsInsideBoxHomeTeam, 'shotsInsideBoxAwayTeam':shotsInsideBoxAwayTeam,
                'totalShotsHomeTeam':totalShotsHomeTeam, 'totalShotsAwayTeam':totalShotsAwayTeam,
                'ballPossessionHomeTeam':ballPossessionHomeTeam, 'ballPossessionAwayTeam':ballPossessionAwayTeam}
        df = df.append(row, ignore_index=True)
    # Sleep 2,1 seconds between each statistics request to avoid the 30 requests / minute API Football limitation
    time.sleep(2.1)
    # Upload statistics as a csv file into 'processed-data' folder
    prefix = 'processed-data/api-football/statistics/'
    name = ''.join(['statistics-', todayDate, '.csv'])
    uploadCsvToS3(df, dataLakeBucketName, s3_client, prefix, name)

    print('Data lake deployed successfully!')

if __name__ == '__main__':
    main()
