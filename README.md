# data_engineer_project
A simple data engineer project to implement some skills

## Project's purpose
I do this project to implement and show some of my data engineer skills. It could also be a fruitful support in order to discuss in an interview.

For this project I will only use cloud services, especially AWS ones.

For the context, let's imagine that we are in a company which wants to understand what makes football fixtures attractive.

To do this, we will create our datalake, where we will store and process all of our data.

To avoid costs and because my purpose is focused on the engineering part, the analysis part will be very restricted.
It will consist in observing the correlation between some basic stats (goals, shots, etc) and the attractiveness of a match based on a sentimental analysis of tweets.
I will focus on english Barclays Premier League, considering all the teams and matchweeks.

## Data

To handle the three existing data type, I will use:
- Structured data: a csv file containing the city of each club
- Semi-structured data: json data from API-Football
- Unstructured data: tweets from the Twitter API

To handle the two existing ingestion modes, data will be ingest by:
- Batch processing: data from API-Football
- Real-time processing (streaming): tweets from Twitter API

### CSV file

Link of the csv file

### APIs

Twitter API
Tweets I want to get

API-Football
Stats I want to get

### Data ingestion
All the data are first stored in their raw format into an S3 bucket which I will call "".

### Which services ?
Glue
Kinesis
Lambda

### Which triggers ?

## Designing data lake
