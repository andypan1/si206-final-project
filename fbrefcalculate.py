import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import numpy as np

# Connect to the database
conn = sqlite3.connect('./db/final_new.db')

# Fetch city IDs from the database
city_ids = pd.read_sql_query("SELECT cid, name FROM cities", conn)
city_name_to_id = dict(zip(city_ids['name'], city_ids['cid']))

# Dictionary to map teams to their home cities
team_to_city = {
    'Manchester United': 'Manchester',
    'Liverpool': 'Liverpool',
    'Arsenal': 'London',
    'West Ham': 'London',
    'Chelsea': 'London',
    'Manchester City': 'Manchester'
}

# Function to calculate monthly goals
def calculate_monthly_goals(data):
    data['Date'] = pd.to_datetime(data['Date'], errors='coerce')
    data['Month'] = data['Date'].dt.month
    data['GF'] = pd.to_numeric(data['GF'], errors='coerce')
    monthly_goals = data.groupby(['Month'])['GF'].mean().reset_index()
    return monthly_goals

# Fetch goals data and store in a dictionary
team_goals = {}
for team in team_to_city.keys():
    team_id_query = f"SELECT team_id FROM teams WHERE team_name = '{team}'"
    team_id = pd.read_sql_query(team_id_query, conn)
    
    if not team_id.empty:
        team_id = team_id.iloc[0]['team_id']
        
        # Query goals data for the team
        query = f"""
        SELECT Date, GF
        FROM team_goals
        WHERE TeamID = {team_id}
        """
        goals_data = pd.read_sql_query(query, conn)
        monthly_goals = calculate_monthly_goals(goals_data)
        team_goals[team] = monthly_goals

# Fetch temperature data using city IDs
team_temps = {}
for team, city_name in team_to_city.items():
    if city_name in city_name_to_id:
        city_id = city_name_to_id[city_name]
        query = f"""
        SELECT month, temp
        FROM monthly_avg_temp
        WHERE city_id = {city_id}
        """
        temp_data = pd.read_sql_query(query, conn)
        team_temps[team] = temp_data

conn.close()

# Aggregate data for plotting and regression
all_temps = []
all_goals = []

csv_data = []  # Prepare for CSV export
for team, city_name in team_to_city.items():
    if team in team_goals and team in team_temps:
        goals = team_goals[team]
        temps = team_temps[team]

        for _, row in goals.iterrows():
            month = row['Month']
            avg_goals = row['GF']
            temp_row = temps[temps['month'] == month]
            if not temp_row.empty:
                temp = temp_row.iloc[0]['temp']
                all_temps.append(temp)
                all_goals.append(avg_goals)
                csv_data.append({'Team': team, 'City': city_name, 'Month': month, 'Temperature': temp, 'Average Goals': avg_goals})

# Convert to numpy arrays for regression
all_temps = np.array(all_temps)
all_goals = np.array(all_goals)

# Create scatter plot
plt.figure(figsize=(12, 8))
for team in team_goals.keys():
    if team in team_temps:
        temps = team_temps[team]
        goals = team_goals[team]

        data_to_plot = []
        for _, row in goals.iterrows():
            month = row['Month']
            avg_goals = row['GF']
            temp_row = temps[temps['month'] == month]
            if not temp_row.empty:
                temp = temp_row.iloc[0]['temp']
                data_to_plot.append((temp, avg_goals))

        if data_to_plot:
            temps, goals = zip(*data_to_plot)
            plt.scatter(temps, goals, label=team, alpha=0.8)

# Add regression line
if len(all_temps) > 0 and len(all_goals) > 0:
    slope, intercept = np.polyfit(all_temps, all_goals, 1)
    plt.plot(
        all_temps,
        slope * all_temps + intercept,
        color='black',
        label="Overall Trend",
        linestyle="-",
        linewidth=1.5
    )

plt.title("Correlation Between Average Home Goals and Temperature (All Teams)")
plt.xlabel("Temperature (°C)")
plt.ylabel("Average Home Goals")
plt.legend(title="Teams")
plt.grid(True)
plt.show()

# Export CSV
csv_df = pd.DataFrame(csv_data)
csv_df.to_csv('team_goals_vs_temperature.csv', index=False)
print("Data written to 'team_goals_vs_temperature.csv'")
