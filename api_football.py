import requests
import datetime

url = "https://api-football-beta.p.rapidapi.com/fixtures"

todayDate = datetime.datetime.today().strftime('%Y-%m-%d')
previousWeek = datetime.datetime.today() - datetime.timedelta(days=7)
previousWeekDate = previousWeek.strftime('%Y-%m-%d')

querystring = {"league":"39", "season":"2020", "from":previousWeekDate, "to":todayDate}

headers = {
    'x-rapidapi-key': "XXX",
    'x-rapidapi-host': "api-football-beta.p.rapidapi.com"
    }

response = requests.request("GET", url, headers=headers, params=querystring)

print(response.text)
