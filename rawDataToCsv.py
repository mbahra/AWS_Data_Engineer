import json
import urllib.parse
import boto3

s3 = boto3.client('s3')

def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    response = s3.get_object(Bucket=bucket, Key=key)

    text = response["Body"].read().decode()
    data = json.loads(text)

    print(key, "has been uploaded into", bucket)

    if "nextWeekFixturesJson" in key:
        # make a csv file with fields: idFixtures, status, date, hour, idHomeTeam, idAwayTeam

    elif "lastWeekFixturesJson" in key:
        # make a csv file with fields: idFixtures, status, date, hour, idHomeTeam, idAwayTeam, goalsHomeTeam, goalsAwayTeam

    elif "statiscsJson" in key:
        # make a csv file with fields: idFixtures, idTeams, Shots on Goal, Shots off Goal, Shots insidebox, Shots outsidebox, Total Shots, Ball Possession
