import http.client
import json
import sqlite3

def process_stats(season, league_id):
    '''
    Process the api from api-football in order to get the standings of the 
    Premier League and La Liga for each year and the team statisitcs for each team in both leagues
    for each year.
    '''
    con = sqlite3.connect('./db/final_new.db')
    cur = con.cursor()

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
    
    # fill TeamStatistics Table
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
            INSERT OR IGNORE INTO TeamStatistics(tid, year, wins, goals_scored, points)
            VALUES(?, ?, ?, ?, ?)
            ''',
            (team_id, season, wins, goals, points, )
        )

    con.commit()
    con.close()

def main():
    years = int(input("Enter a year from 2020-2022: "))
    if years not in [2020, 2021, 2022]:
        print("Please enter a valid year.")
        exit(1)
    league = str(input("Choose a league: La Liga or Premier League: "))
    if league == "La Liga":
        process_stats(years, 140)
    elif league == "Premier League":
        process_stats(years, 39)
    else:
        print("Please enter a supported league.")
        exit(1)

if __name__ == '__main__':
    main()