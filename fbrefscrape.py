import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import time
import matplotlib.pyplot as plt
import numpy as np

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

# Define a function to calculate monthly goals
def calculate_monthly_goals(data):
    data['Date'] = pd.to_datetime(data['Date'], errors='coerce')
    data['Month'] = data['Date'].dt.month
    data['Year'] = data['Date'].dt.year
    data['GF'] = pd.to_numeric(data['GF'], errors='coerce')
    monthly_goals = data.groupby(['Year', 'Month'])['GF'].sum().reset_index()
    return monthly_goals

# Define a function to calculate average home goals
def calculate_avg_home_goals(data):
    data['Date'] = pd.to_datetime(data['Date'], errors='coerce')
    data['Month'] = data['Date'].dt.month
    data['Year'] = data['Date'].dt.year
    home_games = data[data['Venue'] == 'Home'].copy()
    home_games.loc[:, 'GF'] = pd.to_numeric(home_games['GF'], errors='coerce')
    avg_home_goals = home_games.groupby(['Year', 'Month'])['GF'].mean().reset_index()
    avg_home_goals.rename(columns={'GF': 'Avg Home Goals'}, inplace=True)
    return avg_home_goals

# URLs for the teams
team_urls = {
    'Manchester': "https://fbref.com/en/squads/b8fd03ef/2022-2023/matchlogs/all_comps/schedule/Manchester-City-Scores-and-Fixtures-All-Competitions",
    'Liverpool': "https://fbref.com/en/squads/822bd0ba/2022-2023/matchlogs/all_comps/schedule/Liverpool-Scores-and-Fixtures-All-Competitions",
    'Arsenal': "https://fbref.com/en/squads/18bb7c10/2022-2023/matchlogs/all_comps/schedule/Arsenal-Scores-and-Fixtures-All-Competitions",
    'West Ham': "https://fbref.com/en/squads/7c21e445/2022-2023/matchlogs/all_comps/schedule/West-Ham-United-Scores-and-Fixtures-All-Competitions"
}

# Map teams to their home cities
team_to_city = {
    'Manchester': 'Manchester',
    'Liverpool': 'Liverpool',
    'Arsenal': 'London',
    'West Ham': 'London'
}

# Dictionary to store results
team_data = {}

# Scrape data and calculate goals
for team, url in team_urls.items():
    print(f"Scraping data for {team}...")
    team_table = scrape_team_table(url)
    monthly_goals = calculate_monthly_goals(team_table)
    avg_home_goals = calculate_avg_home_goals(team_table)
    team_data[team] = {'monthly_goals': monthly_goals, 'avg_home_goals': avg_home_goals}
    time.sleep(6)  # Wait to avoid exceeding request limits

# Connect to the database
conn = sqlite3.connect('./db/final.db')

# Prepare the SQL query to match cities
cities = "', '".join(team_to_city.values())
query = f"""
SELECT location, month, temp
FROM monthly_avg_temp
WHERE location IN ('{cities}')
ORDER BY location, month
"""

# Execute the query and fetch the results
cursor = conn.cursor()
cursor.execute(query)
temp_data = cursor.fetchall()
conn.close()

# Convert temperatures to a DataFrame
temp_df = pd.DataFrame(temp_data, columns=['Location', 'Month', 'Temperature'])

# Print results for each team
for team, data in team_data.items():
    print(f"\nTeam: {team}")
    print("Monthly Goals:")
    print(data['monthly_goals'])
    print("\nAverage Home Goals:")
    print(data['avg_home_goals'])

    # Fetch temperature data for the team's city
    team_city = team_to_city[team]
    team_temps = temp_df[temp_df['Location'] == team_city]
    print("\nMonthly Temperatures:")
    print(team_temps)


# Aggregate data for all teams
all_temps = []
all_goals = []

for team, data in team_data.items():
    # Merge temperature data with team data
    monthly_goals = data['avg_home_goals']
    city_temps = temp_df[temp_df['Location'] == team_to_city[team]]  # Use existing mapping
    merged_data = monthly_goals.merge(city_temps, on='Month', how='inner')
    
    # Collect data for regression
    all_temps.extend(merged_data['Temperature'])
    all_goals.extend(merged_data['Avg Home Goals'])

# Convert to numpy arrays for regression
all_temps = np.array(all_temps)
all_goals = np.array(all_goals)

# Create scatter plot
plt.figure(figsize=(12, 8))
for team, data in team_data.items():
    # Merge temperature data with team data
    monthly_goals = data['avg_home_goals']
    city_temps = temp_df[temp_df['Location'] == team_to_city[team]]  # Use existing mapping
    merged_data = monthly_goals.merge(city_temps, on='Month', how='inner')

    # Scatter plot for each team
    plt.scatter(
        merged_data['Temperature'],
        merged_data['Avg Home Goals'],
        label=team,
        alpha=0.8
    )

# Add a regression line for all teams
slope, intercept = np.polyfit(all_temps, all_goals, 1)  # Linear regression
plt.plot(
    all_temps,
    slope * all_temps + intercept,
    color='black',
    label="Overall Trend",
    linestyle="-",
    linewidth=1.5
)

# Add labels, title, and legend
plt.title("Correlation Between Average Home Goals and Temperature (All Teams)")
plt.xlabel("Temperature (°C)")
plt.ylabel("Average Home Goals")
plt.legend(title="Teams")
plt.grid(True)
plt.show()
