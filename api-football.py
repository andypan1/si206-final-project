import http.client
import json
import time
import sqlite3


con = sqlite3.connect('./db/final.db')
cur = con.cursor()

cur.execute(
    '''
    CREATE TABLE IF NOT EXISTS Teams(
        team_id INTEGER PRIMARY KEY,
        team_name TEXT UNIQUE,
        home TEXT
    )
    '''
)
cur.execute(
    '''
    CREATE TABLE IF NOT EXISTS TeamStatistics (
        tid INTEGER,
        year INTEGER,
        wins INTEGER,
        goals_scored INTEGER,
        points INTEGER,
        FOREIGN KEY (tid) REFERENCES Teams (team_id),
        PRIMARY KEY (tid, year)
    )
    '''
)

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
    # fill Teams Table
    for season in seasons:
        endpoint = f"/v3/teams?league={league_id}&season={season}"
        conn.request("GET", endpoint, headers=api_headers)
        response = conn.getresponse() 
        data = response.read().decode("utf-8")
        response_data = json.loads(data)
        teams = response_data['response']
        for team in teams:
            team_name = team['team']['name']
            city = team['venue']['city']
            team_id = team['team']['id']
            cur.execute(
                '''
                INSERT OR IGNORE INTO Teams(team_id, team_name, home)
                VALUES(?, ?, ?)
                ''',
                (team_id, team_name, city)
            )
        time.sleep(2)
    
    # fill TeamStatistics Table
    for season in seasons:
        endpoint = f"/v3/standings?league={league_id}&season={season}"
        conn.request("GET", endpoint, headers=api_headers)
        response = conn.getresponse()
        data = response.read().decode("utf-8")
        response_data = json.loads(data)
        standings = response_data['response'][0]['league']['standings'][0]
        for team in standings:
            team_id = team['team']['id']
            points = team['points']
            wins = team['all']['win']
            goals = team['all']['goals']['for']

            cur.execute(
                '''
                INSERT INTO TeamStatistics(tid, year, wins, goals_scored, points)
                VALUES(?, ?, ?, ?, ?)
                ''',
                (team_id, season, wins, goals, points)
            )
        time.sleep(2)

con.commit()
con.close()