import openmeteo_requests # type: ignore

import requests_cache # type: ignore
import pandas as pd # type: ignore
from retry_requests import retry # type: ignore
import sqlite3


def process_weather(cities, name):
    """
    Takes in the cities and latitude/longitude of where each
    Premier team's home stadium is located in and adds the monthly
    weather of each city between 8/22-5/23 into the database.
    """
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://historical-forecast-api.open-meteo.com/v1/forecast"

    conn = sqlite3.connect('./db/final_new.db')
    cursor = conn.cursor()

    cursor.execute(
        '''
        CREATE TABLE IF NOT EXISTS cities(
            cid INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        )
        '''
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS monthly_avg_temp (
            city_id INTEGER,
            month INTEGER,
            temp REAL,
            FOREIGN KEY (city_id) REFERENCES cities (cid),
            PRIMARY KEY (city_id, month)
        )
        """
    )

    params = {
        "latitude": cities[name][0],
        "longitude": cities[name][1],
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


    cursor.execute(
        '''
        INSERT OR IGNORE INTO cities(name)
        VALUES (?)
        ''',
        (name, )
    )

    cid = cursor.execute(
        '''
        SELECT cid FROM cities WHERE name = ?;
        ''',
        (name, )
    ).fetchone()[0]
    
    # Group by year and month and calculate the average
    monthly_avg = daily_dataframe.groupby('year_month')['temperature_2m_max'].mean().reset_index()
    for _, row in monthly_avg.iterrows():
        cursor.execute(
            """
            INSERT OR IGNORE INTO monthly_avg_temp(city_id, month, temp)
            VALUES (?, ?, ?)
            """,
            (cid, int(row['year_month']), round(row['temperature_2m_max'], 4), )
        )

    conn.commit()
    conn.close()

def main():
    cities = {
        "London": (51.50, -0.12),
        "Brentford": (51.48, -0.31),
        "Birmingham": (52.49, -1.90),
        "Bournemouth": (50.73, -1.88),
        "Falmer": (50.86, -0.08),  # Brighton
        "Liverpool": (53.40, -2.98),
        "Manchester": (53.48, -2.24),
        "Leeds": (53.80, -1.55),
        "Newcastle": (54.97, -1.60),
        "Nottingham": (52.95, -1.15),
        "Southampton": (50.90, -1.40),
        "Leicester": (52.63, -1.13),
        "Wolverhampton": (52.59, -2.11)
    }

    print("You can choose from the following cities:")
    print(", ".join(cities.keys()))
    user_input = input("Enter a city name: ").strip()
    if user_input not in cities:
        print("Invalid city. Please try again.")
    process_weather(cities, user_input)

if __name__ == '__main__':
    main()