import sqlite3
import requests # type: ignore
from bs4 import BeautifulSoup # type: ignore
import time
import re
import unicodedata

conn = sqlite3.connect('./db/final.db')
cur = conn.cursor()

cur.execute(
    '''
    CREATE TABLE IF NOT EXISTS transfer_fee(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        team TEXT,
        year INTEGER,
        expenses INTEGER
    )
    '''
)
def clean_team_name(team_name):
    if team_name == "Atlético de Madrid":
        return "Atletico Madrid"
    if team_name == "Tottenham Hotspur":
        return "Tottenham"
    team_name = unicodedata.normalize('NFD', team_name)
    team_name = ''.join(char for char in team_name if unicodedata.category(char) != 'Mn')

    unwanted_prefixes = ["FC", "CF", "UD", "SD", "CA", "RCD"]
    unwanted_suffixes = ["CF", "FC", "UD", "SD", "CA", "RCD"]

    words = team_name.split()
    
    if words and words[0] in unwanted_prefixes:
        words = words[1:]  
    
    if words and words[-1] in unwanted_suffixes:
        words = words[:-1] 
    
    cleaned_name = ' '.join(words)
    
    return cleaned_name.strip()

def convert_euros_to_int(euros):
    if euros == "-":
        return 0
    euros = euros.replace("€", "").replace(",", "").strip()
    if "m" in euros:
        return int(float(euros.replace("m", "").strip()) * 1_000_000)
    if "k" in euros:
        return int(float(euros.replace("k", "").strip()) * 1_000)
    return int(euros)

years = [2020, 2021, 2022]
# 2020-2022 premier league transfers
for year in years:
    url = f"https://www.transfermarkt.us/premier-league/einnahmenausgaben/wettbewerb/GB1/plus/0?ids=a&sa=&saison_id={year}&saison_id_bis={year}&nat=&pos=&altersklasse=&w_s=&leihe=&intern=0"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    }
    resp = requests.get(url, headers=headers)
    if resp.ok:
        soup = BeautifulSoup(resp.content, 'html.parser')
        table_div = soup.find("div", {"id": "yw1"})
        
        teams_data = []
        for tbody in table_div.find_all("tbody"):
            for row in tbody.find_all("tr"):
                team_name_cell = row.find("td", class_="hauptlink no-border-links")
                team_name = team_name_cell.get_text(strip=True)
                team_name = clean_team_name(team_name)
                euros_cell = row.find("td", class_="rechts hauptlink redtext")
                euros_string = euros_cell.get_text(strip=True) if euros_cell else None
                euros_value = convert_euros_to_int(euros_string) if euros_string else 0
                if team_name and euros_value is not None:
                    teams_data.append({"team": team_name, "euros": euros_value})
        for team in teams_data:
            cur.execute(
                '''
                INSERT INTO transfer_fee(team, year, expenses)
                VALUES (?, ?, ?)
                ''',
                (team['team'], year, team['euros'])
            )
    else:
        print(f"Failed to retrieve the page. Status code: {resp.status_code}")
    time.sleep(2)
    
# 2020-2022 la liga transfers
for year in years:
    url = f"https://www.transfermarkt.us/laliga/einnahmenausgaben/wettbewerb/ES1/plus/0?ids=a&sa=&saison_id={year}&saison_id_bis={year}&nat=&pos=&altersklasse=&w_s=&leihe=&intern=0"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    }
    resp = requests.get(url, headers=headers)
    if resp.ok:
        soup = BeautifulSoup(resp.content, 'html.parser')
        table_div = soup.find("div", {"id": "yw1"})
        
        teams_data = []
        for tbody in table_div.find_all("tbody"):
            for row in tbody.find_all("tr"):
                team_name_cell = row.find("td", class_="hauptlink no-border-links")
                team_name = team_name_cell.get_text(strip=True)
                team_name = clean_team_name(team_name)
                euros_cell = row.find("td", class_="rechts hauptlink redtext")
                euros_string = euros_cell.get_text(strip=True)
                euros_value = convert_euros_to_int(euros_string) if euros_string else 0
                if team_name and euros_value is not None:
                    teams_data.append({"team": team_name, "euros": euros_value})
        for team in teams_data:
            cur.execute(
                '''
                INSERT INTO transfer_fee(team, year, expenses)
                VALUES (?, ?, ?)
                ''',
                (team['team'], year, team['euros'])
            )
    else:
        print(f"Failed to retrieve the page. Status code: {resp.status_code}")
    time.sleep(2)

conn.commit()
conn.close()
