import requests
import datetime
import json

url = "https://api-football-beta.p.rapidapi.com/fixtures"

headers = {
'x-rapidapi-key': "XXX", # Write your api key in place of XXX
'x-rapidapi-host': "api-football-beta.p.rapidapi.com"
}

todayDate = datetime.datetime.today().strftime('%Y-%m-%d')
previousWeek = datetime.datetime.today() - datetime.timedelta(days=7)
previousWeekDate = previousWeek.strftime('%Y-%m-%d')

querystring = {"league":"39", "season":"2020", "from":previousWeekDate, "to":todayDate}

response = requests.request("GET", url, headers=headers, params=querystring)

print(response.json())
