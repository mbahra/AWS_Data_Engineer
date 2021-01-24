import requests
import datetime
import json
import csv
import schedule
import time
import logging
import boto3
import uuid
import io
import pandas as pd
from botocore.exceptions import ClientError

s3_client = boto3.client('s3')

bucket = 'datalake-179b45ee-8056-4a30-8be8-1f4076ddbe01'

teamcodes = s3_client.get_object(Bucket=bucket, Key='processed-data/teamcodes.csv')

teamcodesDf = pd.read_csv(teamcodes['Body'])

print(teamcodesDf)
