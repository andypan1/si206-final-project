import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import time

def clear_team_goals_table(conn):
    confirm = input("Do you want to clear the 'team_goals' table? This action cannot be undone (yes/no): ").strip().lower()
    if confirm == 'yes':
        cursor = conn.cursor()
        cursor.execute("DELETE FROM team_goals")
        conn.commit()
        print("The 'team_goals' table has been cleared.")
    else:
        print("Clear operation aborted.")

# Function to prompt user for database name
def get_database_name():
    db_name = input("Enter the name of the database file (e.g., 'my_database.db'): ").strip()
    if not db_name.endswith('.db'):
        print("Invalid file name. Database file must end with '.db'.")
        exit()
    return f"./db/{db_name}"  # Add './db/' prefix

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
    cursor.execute("SELECT team_id FROM Teams WHERE team_name = ?", (team_name,))
    result = cursor.fetchone()
    
    if result:
        return result[0]
    else:
        raise ValueError(f"Team '{team_name}' not found in the database.")

# Function to check if team data already exists
def team_data_exists(team_id, conn):
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM team_goals WHERE TeamId = ?", (team_id,))
    result = cursor.fetchone()
    return result[0] > 0

# Function to store home goals data in the database
def store_home_goals_in_db(data, team_id, conn):
    data['TeamId'] = team_id
    data['Date'] = pd.to_datetime(data['Date'], errors='coerce')
    data['GF'] = pd.to_numeric(data['GF'], errors='coerce')
    data = data.dropna(subset=['Date', 'GF'])
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

# User input for database name

database_name = get_database_name()

# User input for team selection
print("Available teams: Manchester United, Liverpool, Arsenal, West Ham, Chelsea, Manchester City")
selected_team = input("Enter the team name: ").strip()

if selected_team not in team_urls:
    print("Invalid team name. Please choose from the available list.")
else:
    # Connect to the user-specified database
    conn = sqlite3.connect(database_name)
    #clear_team_goals_table(conn)
    try:
        # Retrieve the team ID
        team_id = get_team_id(selected_team, conn)
        
        # Check if data already exists
        if team_data_exists(team_id, conn):
            print(f"Data for {selected_team} already exists in the database. Skipping data insertion.")
        else:
            # Scrape data and store only home goals for the selected team
            print(f"Scraping home goals data for {selected_team}...")
            url = team_urls[selected_team]
            team_table = scrape_team_table(url)
            store_home_goals_in_db(team_table, team_id, conn)
            print(f"Home goals data for {selected_team} successfully stored in the database.")
             # Wait to avoid exceeding request limits
    except ValueError as e:
        print(e)
    finally:
        # Close the database connection
        conn.close()
