import http.client
import json
import sqlite3

def process_team(season, league_id):
    '''
    Process the api from api-football in order to get the standings of the 
    Premier League and La Liga for each year and the team statisitcs for each team in both leagues
    for each year.
    '''
    con = sqlite3.connect('./db/final_new.db')
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

    conn = http.client.HTTPSConnection("api-football-v1.p.rapidapi.com")
    # API headers
    api_headers = {
        'x-rapidapi-host': "api-football-v1.p.rapidapi.com",
        'x-rapidapi-key': "6c04748644mshcbbb06b9b2789bep1102b6jsneee9fc830bb9"
    }

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
            (team_id, team_name, city, )
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
        process_team(years, 140)
    elif league == "Premier League":
        process_team(years, 39)
    else:
        print("Please enter a supported league.")
        exit(1)

if __name__ == '__main__':
    main()