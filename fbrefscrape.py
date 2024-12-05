import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3

def scrape_team_table(url):
    # Fetch the webpage
    response = requests.get(url)
    response.raise_for_status()  # Raise an error if the request fails
    soup = BeautifulSoup(response.text, 'html.parser')

    # Locate the table
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

# Scrape Manchester City's table
man_city_url = "https://fbref.com/en/squads/b8fd03ef/2022-2023/matchlogs/all_comps/schedule/Manchester-City-Scores-and-Fixtures-All-Competitions"
man_city_table = scrape_team_table(man_city_url)
print(man_city_table.head())

def calculate_monthly_goals(data):
    # Convert 'Date' column to datetime
    data['Date'] = pd.to_datetime(data['Date'], errors='coerce')

    # Extract month and year
    data['Month'] = data['Date'].dt.month
    data['Year'] = data['Date'].dt.year

    # Calculate total goals (GF) by month
    data['GF'] = pd.to_numeric(data['GF'], errors='coerce')  # Ensure GF is numeric
    monthly_goals = data.groupby(['Year', 'Month'])['GF'].sum().reset_index()
    return monthly_goals

# Calculate goals for Manchester City
man_city_goals = calculate_monthly_goals(man_city_table)
print(man_city_goals)

def calculate_avg_home_goals(data):
    # Convert 'Date' column to datetime
    data['Date'] = pd.to_datetime(data['Date'], errors='coerce')

    # Extract month and year
    data['Month'] = data['Date'].dt.month
    data['Year'] = data['Date'].dt.year

    # Filter for home games only
    home_games = data[data['Venue'] == 'Home'].copy()  # Create a full copy to avoid SettingWithCopyWarning

    # Ensure GF is numeric
    home_games.loc[:, 'GF'] = pd.to_numeric(home_games['GF'], errors='coerce')

    # Group by Year and Month and calculate the average goals scored at home
    avg_home_goals = home_games.groupby(['Year', 'Month'])['GF'].mean().reset_index()
    avg_home_goals.rename(columns={'GF': 'Avg Home Goals'}, inplace=True)
    
    return avg_home_goals


# Calculate average home goals for Manchester City
avg_home_goals_man_city = calculate_avg_home_goals(man_city_table)
print(avg_home_goals_man_city)
