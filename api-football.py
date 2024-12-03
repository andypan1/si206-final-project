import http.client
import json
import time

conn = http.client.HTTPSConnection("api-football-v1.p.rapidapi.com")
# API headers
api_headers = {
    'x-rapidapi-host': "api-football-v1.p.rapidapi.com",
    'x-rapidapi-key': "6c04748644mshcbbb06b9b2789bep1102b6jsneee9fc830bb9"
}

# Leagues and seasons to process
leagues = [39, 140]  # Premier League and La Liga
seasons = [2020, 2021, 2022]


# Fetch standings for each league and season
for league_id in leagues:
    for season in seasons:
        all_standings = {}
        endpoint = f"/v3/standings?league={league_id}&season={season}"
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
            team_info = {}

            for team_id in team_ids:
                stats_endpoint = f"/v3/teams?id={team_id}"
                conn.request("GET", stats_endpoint, headers=api_headers)
                stats_response = conn.getresponse()
                stats_data = stats_response.read().decode("utf-8")
                time.sleep(2)

                # Parse and store info
                if team_id not in team_info:
                    team_info[team_id] = {}
                team_info[team_id][f"{league_id}_{season}"] = json.loads(stats_data)

            with open(f"{league_id}_{season}_team_stats.json", 'w') as statistics_file:
                json.dump(team_info, statistics_file, indent=4)

        with open(f"{league_id}_{season}_standings.json", 'w') as standings_file:
            json.dump(all_standings, standings_file, indent=4)


    print(f"Standings data saved to {league_id}{season}_standings.json for leagues {leagues} and seasons {seasons}.")
    print(f"Team statistics data saved to {league_id}{season}_team_stats.son")
