import requests # type: ignore
from bs4 import BeautifulSoup # type: ignore
import pandas as pd # type: ignore
import sqlite3
import time
import matplotlib.pyplot as plt # type: ignore
import numpy as np # type: ignore

# Define a function to scrape team tables
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
    return df

# Define a function to store goals data in the database
def store_goals_in_db(data, team_name, conn):
    # Select relevant columns (modify as per the table structure in fbref)
    data = data[['Date', 'GF', 'Venue']].copy()
    data['Team'] = team_name

    # Convert to datetime and numeric where applicable
    data['Date'] = pd.to_datetime(data['Date'], errors='coerce')
    data['GF'] = pd.to_numeric(data['GF'], errors='coerce')

    # Drop rows with invalid dates or goals
    data = data.dropna(subset=['Date', 'GF'])

    # Insert data into the database
    data.to_sql('team_goals', conn, if_exists='append', index=False)

# URLs for the teams
team_urls = {
    'Manchester': "https://fbref.com/en/squads/b8fd03ef/2022-2023/matchlogs/all_comps/schedule/Manchester-City-Scores-and-Fixtures-All-Competitions",
    'Liverpool': "https://fbref.com/en/squads/822bd0ba/2022-2023/matchlogs/all_comps/schedule/Liverpool-Scores-and-Fixtures-All-Competitions",
    'Arsenal': "https://fbref.com/en/squads/18bb7c10/2022-2023/matchlogs/all_comps/schedule/Arsenal-Scores-and-Fixtures-All-Competitions",
    'West Ham': "https://fbref.com/en/squads/7c21e445/2022-2023/matchlogs/all_comps/schedule/West-Ham-United-Scores-and-Fixtures-All-Competitions"
}

# Connect to the database
conn = sqlite3.connect('./db/final.db')

# Create the `team_goals` table if it doesn't exist
create_table_query = """
CREATE TABLE IF NOT EXISTS team_goals (
    Date DATE,
    GF INTEGER,
    Venue TEXT,
    Team TEXT
);
"""
cursor = conn.cursor()
cursor.execute(create_table_query)
conn.commit()

# Scrape data and store goals in the database
for team, url in team_urls.items():
    print(f"Scraping data for {team}...")
    team_table = scrape_team_table(url)
    store_goals_in_db(team_table, team, conn)
    time.sleep(6)  # Wait to avoid exceeding request limits

# Close the database connection
conn.close()

print("Goals data successfully stored in the database.")
