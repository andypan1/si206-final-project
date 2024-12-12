import pandas as pd # type: ignore
import sqlite3 # type: ignore
import matplotlib.pyplot as plt # type: ignore
import numpy as np # type: ignore

# Connect to the database
conn = sqlite3.connect('./db/final.db')

# Dictionary to map teams to their home cities
team_to_city = {
    'Manchester': 'Manchester',
    'Liverpool': 'Liverpool',
    'Arsenal': 'London',
    'West Ham': 'London'
}

# Function to calculate monthly goals
def calculate_monthly_goals(data):
    data['Date'] = pd.to_datetime(data['Date'], errors='coerce')
    data['Month'] = data['Date'].dt.month
    data['Year'] = data['Date'].dt.year
    data['GF'] = pd.to_numeric(data['GF'], errors='coerce')
    monthly_goals = data.groupby(['Month'])['GF'].mean().reset_index()  # Average goals per month
    return monthly_goals

# Fetch goals data and store in a dictionary
team_goals = {}
for team in team_to_city.keys():
    query = f"""
    SELECT Date, GF, Venue
    FROM team_goals
    WHERE Team = '{team}'
    """
    goals_data = pd.read_sql_query(query, conn)
    monthly_goals = calculate_monthly_goals(goals_data)
    team_goals[team] = monthly_goals

# Fetch temperature data and store in a dictionary
team_temps = {}
for city in set(team_to_city.values()):
    query = f"""
    SELECT month, temp
    FROM monthly_avg_temp
    WHERE location = '{city}'
    """
    temp_data = pd.read_sql_query(query, conn)
    team_temps[city] = temp_data

conn.close()

# Aggregate data for plotting and regression
all_temps = []
all_goals = []

# Prepare data for CSV output
csv_data = []

# Collect data for plotting and regression
for team, city in team_to_city.items():
    if team in team_goals and city in team_temps:
        # Match on the month
        goals = team_goals[team]
        temps = team_temps[city]

        for _, row in goals.iterrows():
            month = row['Month']
            avg_goals = row['GF']
            temp_row = temps[temps['month'] == month]
            if not temp_row.empty:
                temp = temp_row.iloc[0]['temp']
                all_temps.append(temp)
                all_goals.append(avg_goals)
                csv_data.append({'Team': team, 'City': city, 'Month': month, 'Temperature': temp, 'Average Goals': avg_goals})

# Convert to numpy arrays for regression
all_temps = np.array(all_temps)
all_goals = np.array(all_goals)

# Create scatter plot
plt.figure(figsize=(12, 8))
for team, city in team_to_city.items():
    if team in team_goals and city in team_temps:
        goals = team_goals[team]
        temps = team_temps[city]
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

# Add a regression line for all teams
if len(all_temps) > 0 and len(all_goals) > 0:
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

# Write the calculations into a CSV file
csv_df = pd.DataFrame(csv_data)
csv_df.to_csv('team_goals_vs_temperature.csv', index=False)

print("Data written to 'team_goals_vs_temperature.csv'")
