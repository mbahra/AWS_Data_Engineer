# data_engineer_project (in progress)
A simple data engineer project to implement some skills

## Project's purpose
I do this project to implement some of my data engineer skills. It could also be a fruitful support in order to discuss in an interview.

For this project I will only use cloud services, especially AWS ones.

For the context, let's imagine that we are in a company which wants to analyze the difference between expected goals and points, and real goals and points for each football team in a same league.

For this project I will create a data lake, where I will store and process data before loading them weekly into a database.
Because my purpose is focused on the engineering part, the analysis part will be very restricted.

I will focus on english Barclays Premier League, considering all the teams and matchweeks for the current season (2020/2021).

## Prerequisites

If you want to run this project by yourself, these are the prerequisites:

- Python 3: https://www.python.org/downloads/

- Pandas library: https://pandas.pydata.org/pandas-docs/stable/getting_started/install.html

- Requests library:
```shell
$ pip install requests
```

- An AWS account: https://portal.aws.amazon.com/billing/signup#/start

- AWS CLI version 2: https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html

- Boto3 SDK:
```shell
$ pip install boto3
```

- A RapidAPI account and key: https://rapidapi.com/marketplace

## AWS Free Tier usage alerts

Pay attention to the pricing conditions. The AWS Free Tier conditions are provided here :
https://aws.amazon.com/free/?nc1=h_ls&all-free-tier.sort-by=item.additionalFields.SortRank&all-free-tier.sort-order=asc

To minimize cost, I recommend you to clean up resources as soon as you finish this project.

To opt in to the AWS Free Tier usage alerts, sign in to the AWS Management Console and open the Billing and Cost Management console at https://console.aws.amazon.com/billing/.
Under Preferences in the navigation pane, choose Billing preferences.
Under Cost Management Preferences, select Receive AWS Free Tier Usage Alerts to opt in to Free Tier usage alerts. To opt out, clear the Receive AWS Free Tier Usage Alerts check box.

## Data lake deployment

![](images/datalakeDeployment.png)

To create S3 bucket and upload files into it with running my python scripts locally, I use the boto3 SDK.

### Creation of a new AWS IAM user

To make boto3 run against my AWS account, I’ll need to provide some valid credentials. If you already have an IAM user that has full permissions to S3, you can use those user’s credentials (their access key and their secret access key) without needing to create a new user. Otherwise, we have to create a new AWS user and then store the new credentials.

To create a new user, I have to use AWS Identity and Access Management (IAM).

I give the user a name (in my case, boto3user), and enable programmatic access to ensure that this user will be able to work with any AWS supported SDK or make separate API calls.

To keep things simple, I choose the preconfigured AmazonS3FullAccess policy. With this policy, the new user will be able to have full control over S3.

At the last user creation step, a new screen shows the user’s generated credentials. I click on the Download .csv button to make a copy of the credentials.

Now that I have my new user, I run the following command to complete my setup:
```shell
$ aws configure
```
I fill in the requested information with the corresponding values from my csv file.
For the Default region name, I select my region using https://docs.aws.amazon.com/fr_fr/general/latest/gr/rande.html#s3_region. In my case, I am using eu-west-3 (Paris).
For the default output format, I select json. The different formats are provided at https://docs.aws.amazon.com/fr_fr/cli/latest/userguide/cli-configure-quickstart.html#cli-configure-quickstart-config.

### Data lake structure

![](images/datalakeStructure.png)

I get football data using API-Football. Here is its documentation: https://www.api-football.com/documentation-v3.

After creating my RapidAPI account and getting my API key (https://rapidapi.com/marketplace), I get the english Barclays Premier League id by sending a request to the API. The id is 39. Now I can write my python script datalakeDeployment.py to get data from API-Football and deploy my data lake by running the script locally.

This script create my data lake as an S3 bucket named with a globally unique name to satisfy S3 policy requirements.
Then it uploads teamcodes.csv to the data lake, into a folder named "processed-data". I made this csv file myself by aggregating the API-Football id, the name, and the team code (for example 'ARS' for Arsenal) of each team. It could be useful to go further in this project by getting some tweets for sentimental analysis.
After that, the script requests API-Football to get previous fixtures, their statistics, and the next week fixtures. Data are uploaded in their json raw format as json files to the data lake into the folders "raw-data/api-football/previous-fixtures", "raw-data/api-football/statistics", and "raw-data/api-football/next-fixtures". Finally, the json data are processed to be uploaded as csv files to the data lake into the folders "processed-data/api-football/previous-fixtures", "processed-data/api-football/statistics", and "processed-data/api-football/next-fixtures". As for teamcodes.csv, I want to get the next week fixtures to be able to go further in this project by handle tweets streaming for some fixtures.

If you want to run the script by yourself, make sure that you filled your API key in place of 'XXX'. Also, pay attention to the API-Football pricing (free until 100 requests per day, around €0.00450 / request beyond). Since the script will send one request to get the previous fixtures, another one to get the next week fixtures, then another one to each of the previous fixtures to get their statistics, you will begin to pay around €0.00450 / fixture for each fixture after the 98 firsts.

## ETL jobs with AWS Lambda

With AWS, there are several ways to perform ETL jobs. You can for example use AWS Glue, which is a serverless data integration service, but also AWS Lambda, which is a serverless compute service. For this project I will use them both. I will use AWS Lambda for the firsts ETL jobs I will create, to show two different ways to run a Lambda function automatically: whith a scheduler, and with a trigger from S3.

#### Adding layers to Lambda functions

In my scripts that I want to run as Lambda functions, I use the requests and pandas packages, which are not directly available in Lambda.
In order to use any of these packages in a Lambda function, I have to add a layer to the Lambda function. For example, if I have a Lambda function using pandas, I have to add it a pandas layer to allow the pandas package import.

Whenever it is needed, I add the required layer using the Klayers repository on github:
https://github.com/keithrozario/Klayers

For my Lambda functions I have to find the right ARN (AWS Resource Name) inside the deployments folder for python3.8.
After checking the region mentioned in my Lambda function ARN, on the top-right corner of the Lambda function screen (in my case 'eu-west-3'),I select the corresponding csv file available in the repository.
Finally, I just have to copy paste the ARN of the layer that I need.

If you want more details go on https://medium.com/@melissa_89553/how-to-import-python-packages-in-aws-lambda-pandas-scipy-numpy-bb2c98c974e9.

#### etlApiFootballRequests job

As I did to deploy the data lake, I want to extract data from API-Football and upload them to the data lake. I want to extract data about the previous week fixtures, their statistics, and the next week fixtures.

To do that, I first create an new IAM role to give AmazonS3FullAccess and AWSLambdaBasicExecutionRole permissions to Lambda.
Then, I create a new python Lambda function that I name etlApiFootballRequests, giving the IAM role I've just created.
I wrote the python script for this Lambda function in etlApiFootballRequests.py. I just have to copy paste the whole script into my new Lambda function and add the requests layer to my Lambda function.

##### Schedule the Lambda function

I schedule my etlApiFootballRequests Lambda function using CloudWatch, with a cron expression.
I schedule this ETL job each Tuesday at 8 AM for years 2020 and 2021, with the cron expression "0 8 ? * TUE 2020-2021".

![](images/etlApiFootballRequests.PNG)

#### etlJsonToCsv jobs

Now that my first job is created and scheduled to get data from API-Football each Tuesday at 8 AM, I will create new ETL jobs with Lambda to process each json file uploaded into the raw-data folder of my data lake bucket, then upload the processed data as a csv file into the processed-data folder.

I wrote a script for each type of data from API-Football: previous fixtures (etlJsonToCsvPreviousFixtures.py), statistics (etlJsonToCsvStatistics.py), and next fixtures (etlJsonToCsvNextFixtures.py).

As previously, I just have to create a new Lambda function for each of these scripts, giving them the IAM role created for Lambda, and copy paste my python scripts to the appropriate Lambda function.

##### Trigger the Lambda functions

To trigger my Lambda functions each time that an object is putted into my data lake, I have to create event notifications into the properties of my data lake bucket.

As shown in my following screenshots, I select the right prefix, suffix, event type, and Lambda function for each trigger.

![](images/lambdaTrigger1.PNG)

![](images/lambdaTrigger2.PNG)

![](images/lambdaTrigger3.PNG)

#### CloudWatch metrics and logs

On the monitoring screen of a Lambda function, I have some views of several metrics and logs, provided by CloudWatch, the monitoring and observability service on AWS.
I can use these views and go to the CloudWatch dashboard to monitor my jobs.

![](images/CloudWatch1.PNG)

![](images/CloudWatch2.PNG)

![](images/CloudWatch3.PNG)
