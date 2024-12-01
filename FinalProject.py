import http.client
import json
import time

# API headers
api_headers = {
    'x-rapidapi-host': "v3.football.api-sports.io",
    'x-rapidapi-key': "d39362e28da0ae4d374f3edf0a31e5dd"
}

# Leagues and seasons to process
leagues = [39, 140]  # Premier League and La Liga, for example
seasons = [2020, 2021, 2022]

# Initialize storage for standings and statistics
all_standings = {}
team_statistics = {}

# Fetch standings for each league and season
for league_id in leagues:
    for season in seasons:
        # Request standings for the league and season
        conn = http.client.HTTPSConnection("v3.football.api-sports.io")
        endpoint = f"/standings?league={league_id}&season={season}"
        conn.request("GET", endpoint, headers=api_headers)
        response = conn.getresponse()
        data = response.read().decode("utf-8")

        # Parse and store standings
        standings = json.loads(data)
        all_standings[f"{league_id}_{season}"] = standings

        # Extract team IDs from standings
        if standings.get("response"):
            teams = standings["response"][0]["league"]["standings"][0]
            team_ids = [team["team"]["id"] for team in teams]

            # Fetch statistics for each team
            for team_id in team_ids:
                time.sleep(7)
                conn = http.client.HTTPSConnection("v3.football.api-sports.io")
                stats_endpoint = f"/teams/statistics?season={season}&team={team_id}&league={league_id}"
                conn.request("GET", stats_endpoint, headers=api_headers)
                stats_response = conn.getresponse()
                stats_data = stats_response.read().decode("utf-8")

                # Parse and store team statistics
                if team_id not in team_statistics:
                    team_statistics[team_id] = {}
                team_statistics[team_id][f"{league_id}_{season}"] = json.loads(stats_data)

# Save standings to standings.json
with open('standings.json', 'w') as standings_file:
    json.dump(all_standings, standings_file, indent=4)

# Save team statistics to team_statistics.json
with open('team_statistics.json', 'w') as statistics_file:
    json.dump(team_statistics, statistics_file, indent=4)

print(f"Standings data saved to standings.json for leagues {leagues} and seasons {seasons}.")
print(f"Team statistics data saved to team_statistics.json.")
