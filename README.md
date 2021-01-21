# data_engineer_project (in progress)
A simple data engineer project to implement some skills

## Project's purpose
I do this project to implement some of my data engineer skills. It could also be a fruitful support in order to discuss in an interview.

For this project I will only use cloud services, especially AWS ones.

For the context, let's imagine that we are in a company which wants to understand what makes football fixtures attractive.

For this project I will create a datalake, where I will store and process data.

To avoid costs and because my purpose is focused on the engineering part, the analysis part will be very restricted.
It will consist in observing the correlation between some basic stats (goals, shots, etc.) and the attractiveness of a match based on a sentimental analysis of tweets.
I will focus on english Barclays Premier League, considering all the teams and matchweeks for the current season (2020/2021).

## Prerequisites

If you want to run this project by yourself, these are the prerequisites:

- Python 3: https://www.python.org/downloads/

- Pandas library: https://pandas.pydata.org/pandas-docs/stable/getting_started/install.html

- Schedule library:
```shell
$ pip install schedule
```

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

- A Twitter account and an access to the Twitter API v2 Early Access:
https://developer.twitter.com/en/products/twitter-api/early-access
https://developer.twitter.com/en/docs/twitter-api/getting-started/guide

## AWS Free Tier usage alerts

Pay attention to the pricing conditions. The AWS Free Tier conditions are provided here :
https://aws.amazon.com/free/?nc1=h_ls&all-free-tier.sort-by=item.additionalFields.SortRank&all-free-tier.sort-order=asc

To minimize cost, I recommend you to clean up resources as soon as you finish this project.

To opt in to the AWS Free Tier usage alerts, sign in to the AWS Management Console and open the Billing and Cost Management console at https://console.aws.amazon.com/billing/.
Under Preferences in the navigation pane, choose Billing preferences.
Under Cost Management Preferences, select Receive AWS Free Tier Usage Alerts to opt in to Free Tier usage alerts. To opt out, clear the Receive AWS Free Tier Usage Alerts check box.

## Creation of a new AWS IAM user

To create S3 bucket and upload files into it with running my python scripts locally, I use the boto3 SDK.

To make boto3 run against my AWS account, I’ll need to provide some valid credentials. If you already have an IAM user that has full permissions to S3, you can use those user’s credentials (their access key and their secret access key) without needing to create a new user. Otherwise, we have to create a new AWS user and then store the new credentials.

To create a new user, I have to use AWS Identity and Access Management (IAM).

I give the user a name (for example, boto3user), and enable programmatic access to ensure that this user will be able to work with any AWS supported SDK or make separate API calls.

To keep things simple, I choose the preconfigured AmazonS3FullAccess policy. With this policy, the new user will be able to have full control over S3.

At the last user creation step, a new screen shows the user’s generated credentials. I click on the Download .csv button to make a copy of the credentials.

Now that I have my new user, I run the following command to complete my setup:
```shell
$ aws configure
```
I fill in the requested information with the corresponding values from my csv file.
For the Default region name, select your region with https://docs.aws.amazon.com/fr_fr/general/latest/gr/rande.html#s3_region. In my case, I am using eu-west-3 (Paris).
For the default output format, select json. You can see the different formats at https://docs.aws.amazon.com/fr_fr/cli/latest/userguide/cli-configure-quickstart.html#cli-configure-quickstart-config.

## Data lake deployment
