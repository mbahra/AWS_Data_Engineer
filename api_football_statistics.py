import requests

fixtureId =
url = "https://api-football-beta.p.rapidapi.com/fixtures/statistics/fixture/" + fixtureId + "/"

headers = {
'x-rapidapi-key': "XXX", # Write your api key in place of XXX
'x-rapidapi-host': "api-football-beta.p.rapidapi.com"
}

response = requests.request("GET", url, headers=headers)

print(response.json())
