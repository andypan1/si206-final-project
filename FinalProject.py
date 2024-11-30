import http.client
import json

# API headers
headers = {
    'x-rapidapi-host': "v3.football.api-sports.io",
    'x-rapidapi-key': "d39362e28da0ae4d374f3edf0a31e5dd"
}

# Request 1: Venue details
conn1 = http.client.HTTPSConnection("v3.football.api-sports.io")
conn1.request("GET", "/standings?league=39&season=2020", headers=headers)
res1 = conn1.getresponse()
data1 = res1.read()

# Save response 1 to JSON file
with open('standings.json', 'w') as file1:
    json.dump(json.loads(data1.decode("utf-8")), file1, indent=4)

# Request 2: Team statistics
conn2 = http.client.HTTPSConnection("v3.football.api-sports.io")
conn2.request("GET", "/teams/statistics?season=2020&team=33&league=39", headers=headers)
res2 = conn2.getresponse()
data2 = res2.read()

# Save response 2 to JSON file
with open('team_statistics.json', 'w') as file2:
    json.dump(json.loads(data2.decode("utf-8")), file2, indent=4)

# Request 3: League seasons
conn3 = http.client.HTTPSConnection("v3.football.api-sports.io")
conn3.request("GET", "/leagues/seasons", headers=headers)
res3 = conn3.getresponse()
data3 = res3.read()

# Save response 3 to JSON file
with open('league_seasons.json', 'w') as file3:
    json.dump(json.loads(data3.decode("utf-8")), file3, indent=4)

import json

# Load JSON data from standings.json file
with open('standings.json', 'r') as file:
    data = json.load(file)

# Extract league information
league_id = data['response'][0]['league']['id']

# Extract standings
standings = data['response'][0]['league']['standings'][0]

# Iterate through each team's standings and extract the required data
for team in standings:
    team_id = team['team']['id']
    points_for = team['all']['goals']['for']
    points_against = team['all']['goals']['against']
    print(f"League ID: {league_id}, Team ID: {team_id}, Points For: {points_for}, Points Against: {points_against}")
