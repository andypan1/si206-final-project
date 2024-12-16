import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import time

# Function to scrape team tables
def scrape_team_table(url):
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')

    table = soup.find('table', id='matchlogs_for')

    # Extract headers
    headers = [th.text.strip() for th in table.find('thead').find_all('th')]

    # Extract rows
    rows = []
    for row in table.find('tbody').find_all('tr'):
        cells = row.find_all(['th', 'td'])
        row_data = [cell.text.strip() for cell in cells]
        rows.append(row_data)

    # Convert to DataFrame
    df = pd.DataFrame(rows, columns=headers)
    
    # Filter only home matches and relevant columns
    df = df[df['Venue'] == 'Home']
    return df[['Date', 'GF']]

# Function to get team ID from the Teams table
def get_team_id(team_name, conn):
    cursor = conn.cursor()
    
    # Fetch the team_id by matching the team_name
    cursor.execute("SELECT team_id FROM Teams WHERE team_name = ?", (team_name,))
    result = cursor.fetchone()
    
    if result:
        return result[0]  # Return existing team_id
    else:
        raise ValueError(f"Team '{team_name}' not found in the database.")

# Function to store home goals data in the database
def store_home_goals_in_db(data, team_id, conn):
    # Add a team ID column
    data['TeamId'] = team_id

    # Convert to datetime and numeric where applicable
    data['Date'] = pd.to_datetime(data['Date'], errors='coerce')
    data['GF'] = pd.to_numeric(data['GF'], errors='coerce')

    # Drop rows with invalid dates or goals
    data = data.dropna(subset=['Date', 'GF'])

    # Insert data into the database
    data.to_sql('team_goals', conn, if_exists='append', index=False)

# URLs for the teams
team_urls = {
    'Manchester United': "https://fbref.com/en/squads/19538871/2022-2023/matchlogs/c9/schedule/Manchester-Scores-and-Fixtures-Premier-League",
    'Liverpool': "https://fbref.com/en/squads/822bd0ba/2022-2023/matchlogs/c9/schedule/Liverpool-Scores-and-Fixtures-Premier-League",
    'Arsenal': "https://fbref.com/en/squads/18bb7c10/2022-2023/matchlogs/c9/schedule/Arsenal-Scores-and-Fixtures-Premier-League",
    'West Ham': "https://fbref.com/en/squads/7c21e445/2022-2023/matchlogs/c9/schedule/West-Ham-Scores-and-Fixtures-Premier-League",
    'Chelsea': "https://fbref.com/en/squads/cff3d9bb/2022-2023/matchlogs/c9/schedule/Chelsea-Scores-and-Fixtures-Premier-League",
    'Manchester City': "https://fbref.com/en/squads/b8fd03ef/2022-2023/matchlogs/c9/schedule/Manchester-City-Scores-and-Fixtures-Premier-League"
}

# User input for team selection
print("Available teams: Manchester United, Liverpool, Arsenal, West Ham, Chelsea, Manchester City")
selected_team = input("Enter the team name: ").strip()

if selected_team not in team_urls:
    print("Invalid team name. Please choose from the available list.")
else:
    # Connect to the database
    conn = sqlite3.connect('./db/final_new.db')

    # Retrieve the team ID
    try:
        team_id = get_team_id(selected_team, conn)
    except ValueError as e:
        print(e)
        conn.close()
        exit()

    # Scrape data and store only home goals for the selected team
    print(f"Scraping home goals data for {selected_team}...")
    url = team_urls[selected_team]
    team_table = scrape_team_table(url)
    store_home_goals_in_db(team_table, team_id, conn)
    time.sleep(3)  # Wait to avoid exceeding request limits

    # Close the database connection
    conn.close()

    print(f"Home goals data for {selected_team} successfully stored in the database.")
