import openmeteo_requests # type: ignore

import requests_cache # type: ignore
import pandas as pd # type: ignore
from retry_requests import retry # type: ignore
import sqlite3



# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
cities = [
    "London",
    "Brentford",
    "Birmingham", 
    "Bournemouth", 
    "Falmer",  #Brighton
    "Liverpool", 
    "Manchester", 
    "Leeds", 
    "Newcastle", 
    "Nottingham", 
    "Southampton", 
    "Leicester", 
    "Wolverhampton"
]
lat_log = [
    (51.50, -0.12),           # London
    (51.48, -0.31),           # Brentford
    (52.49, -1.90),           # Birmingham
    (50.73, -1.88),           # Bournemouth
    (50.86, -0.08),           # Falmer (Brighton)
    (53.40, -2.98),           # Liverpool
    (53.48, -2.24),           # Manchester
    (53.80, -1.55),           # Leeds
    (54.97, -1.60),           # Newcastle
    (52.95, -1.15),           # Nottingham
    (50.90, -1.40),           # Southampton
    (52.63, -1.13),           # Leicester
    (52.59, -2.11)            # Wolverhampton
] # (Source: https://www.latlong.net/category/cities-235-15.html)

conn = sqlite3.connect('./db/final.db')
cursor = conn.cursor()

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS monthly_avg_temp (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        month INTERGER,
        location TEXT,
        temp REAL
    )
    """
)

for i in range(len(cities)):
    params = {
        "latitude": lat_log[i][0],
        "longitude": lat_log[i][1],
        "start_date": "2022-08-01",
        "end_date": "2023-05-31",
        "daily": "temperature_2m_max",
        "temperature_unit": "fahrenheit",
        "timezone": "Europe/London"
    }
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]
    daily = response.Daily()
    daily_temperature_2m_max = daily.Variables(0).ValuesAsNumpy()

    daily_data = {"date": pd.date_range(
        start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
        end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = daily.Interval()),
        inclusive = "left"
    )}
    daily_data["temperature_2m_max"] = daily_temperature_2m_max
    daily_dataframe = pd.DataFrame(data = daily_data)
    daily_dataframe['date'] = pd.to_datetime(daily_dataframe['date'])
    daily_dataframe['year_month'] = daily_dataframe['date'].dt.month 

    # Group by year and month and calculate the average
    monthly_avg = daily_dataframe.groupby('year_month')['temperature_2m_max'].mean().reset_index()
    for _, row in monthly_avg.iterrows():
        cursor.execute(
            """
            INSERT INTO monthly_avg_temp(month, location, temp)
            VALUES (?, ? , ?)
            """,
            (int(row['year_month']), cities[i], round(row['temperature_2m_max'], 4))
        )

conn.commit()
conn.close()
