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

# Load JSON data from standings.json file
with open('standings.json', 'r') as file:
    standings_data = json.load(file)

# Extract team IDs from standings
team_ids = []
standings = standings_data['response'][0]['league']['standings'][0]
for team in standings:
    team_id = team['team']['id']
    team_ids.append(team_id)

# Prepare to fetch team statistics
team_statistics = {}
api_headers = {
    'x-rapidapi-host': "v3.football.api-sports.io",
    'x-rapidapi-key': "d39362e28da0ae4d374f3edf0a31e5dd"
}

# Loop through team IDs and fetch data from the team_statistics API
for team_id in team_ids:
    conn = http.client.HTTPSConnection("v3.football.api-sports.io")
    endpoint = f"/teams/statistics?season=2020&team={team_id}&league=39"  # Adjust season and league as needed
    conn.request("GET", endpoint, headers=api_headers)

    # Get response and decode it
    response = conn.getresponse()
    data = response.read().decode("utf-8")

    # Parse JSON and add it to team_statistics
    team_statistics[team_id] = json.loads(data)

# Save the team statistics to team_statistics.json
with open('team_statistics.json', 'w') as outfile:
    json.dump(team_statistics, outfile, indent=4)

print(f"Team statistics data saved to team_statistics.json for {len(team_ids)} teams.")


# Request 2: Team statistics
# conn2 = http.client.HTTPSConnection("v3.football.api-sports.io")
# conn2.request("GET", "/teams/statistics?season=2020&team=33&league=39", headers=headers)
# res2 = conn2.getresponse()
# data2 = res2.read()

# Save response 2 to JSON file
# with open('team_statistics.json', 'w') as file2:
#     json.dump(json.loads(data2.decode("utf-8")), file2, indent=4)

# Load JSON data from standings.json file
# with open('standings.json', 'r') as file:
#     data = json.load(file)

# Extract league information
# league_id = data['response'][0]['league']['id']

# Extract standings
# standings = data['response'][0]['league']['standings'][0]

# Iterate through each team's standings and extract the required data
# for team in standings:
#     team_id = team['team']['id']
#     points_for = team['all']['goals']['for']
#     points_against = team['all']['goals']['against']
