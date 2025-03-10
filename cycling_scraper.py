import mysql.connector
import pandas as pd
import numpy as np
import os
import re
from bs4 import BeautifulSoup
import requests
from urllib.parse import quote
from dotenv import load_dotenv  # ✅ Load credentials from .env file

# ✅ Load environment variables
load_dotenv()

# ✅ Secure MySQL Credentials
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_HOST = os.getenv("MYSQL_HOST")

if not all([MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_HOST]):
    raise ValueError("❌ Missing one or more required MySQL environment variables!")

def get_text_or_none(cols, index):
    """Helper function to extract text or return None if missing."""
    return cols[index].text.strip() if len(cols) > index and cols[index].text.strip() else None

def format_time(value):
    """Ensure time is stored in hh:mm:ss format, or return None if invalid."""
    if value and re.match(r"^\+?\d+:\d+:\d+$", value):
        return value.replace("+", "")  # Remove '+' sign if present
    return None  # Return None for invalid time formats

# ✅ Get race name and optional year from user input
race_name = input("Enter race name: ").strip()
race_year = input("Enter race year (Leave blank for latest year): ").strip() or "2024"

# ✅ Encode race name for URL
encoded_race_name = quote(race_name.replace(" ", "-").lower())

base_url = f"https://www.procyclingstats.com/race/{encoded_race_name}/{race_year}"
print(f"🔍 Checking if race page exists: {base_url}")

# ✅ Check if the base race page exists
response = requests.get(base_url)
if response.status_code != 200:
    print(f"❌ Error: No valid race page found for {race_name} {race_year}.")
    exit()

# ✅ Try different variations of results pages
valid_url = None
url_variations = [
    f"{base_url}/result",  # One-day races
    f"{base_url}/gc",  # General Classification (Stage races)
    f"{base_url}/stage-1",  # First stage of multi-stage races
]

for url in url_variations:
    response = requests.get(url)
    if response.status_code == 200 and "Page not found" not in response.text:
        valid_url = url
        print(f"✅ Found valid results page: {valid_url}")
        break

if not valid_url:
    print(f"❌ Error: Could not determine a valid results page for {race_name} {race_year}.")
    exit()

# ✅ Fetch full page content
response = requests.get(valid_url)
soup = BeautifulSoup(response.text, "html.parser")

# ✅ Find the results section dynamically
possible_classes = ["result-cont", "basic_table", "pcs-table"]
results_container = None

for cls in possible_classes:
    results_container = soup.find("div", class_=cls) or soup.find("table", class_=cls)
    if results_container:
        break

if not results_container:
    print(f"❌ Error: Could not find a results table on {valid_url}.")
    exit()

# ✅ Extract race results dynamically
results = []
rows = results_container.find_all("tr")

for row in rows:
    cols = row.find_all(["td", "div"])
    if len(cols) < 4:
        continue  # Skip incomplete rows

    position = get_text_or_none(cols, 0)
    rider_name_element = cols[1].find("a")
    rider_name = rider_name_element.text.strip() if rider_name_element else get_text_or_none(cols, 1)
    team_name = get_text_or_none(cols, 2)
    time_gap = format_time(get_text_or_none(cols, 3))  # ✅ Format time properly
    avg_speed = get_text_or_none(cols, 4)
    elevation_gain = get_text_or_none(cols, 5)
    stage_type = get_text_or_none(cols, 6)

    # ✅ Ignore "DNF", "DNS", and missing results
    if not rider_name or "DNF" in rider_name or "DNS" in rider_name or team_name is None:
        continue  # Skip this row

    # ✅ Ensure elevation_gain is numeric
    if elevation_gain:
        elevation_gain = ''.join(filter(str.isdigit, elevation_gain))
        elevation_gain = int(elevation_gain) if elevation_gain else None

    # ✅ Handle missing average speed and stage type
    avg_speed = float(avg_speed) if avg_speed and avg_speed.replace('.', '', 1).isdigit() else None
    stage_type = stage_type if stage_type else "Unknown"

    results.append({
        "Position": position,
        "rider_name": rider_name,  # ✅ Matches MySQL table column
        "Team": team_name,
        "Time": time_gap,
        "Avg Speed": avg_speed,
        "Elevation Gain": elevation_gain,
        "Stage Type": stage_type,
        "Race Name": race_name.title()
    })

# ✅ Convert to Pandas DataFrame
df = pd.DataFrame(results)
if df.empty:
    print("❌ No results found or error occurred.")
    exit()

df["Position"] = pd.to_numeric(df["Position"], errors="coerce")
df["Avg Speed"] = pd.to_numeric(df["Avg Speed"], errors="coerce")
df.replace({np.nan: None}, inplace=True)

# ✅ Print DataFrame Preview
print("✅ Data Extracted Successfully:")
print(df.head())

try:
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )
    cursor = conn.cursor()

    # ✅ Modify database to include additional fields
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS race_results (
            id INT AUTO_INCREMENT PRIMARY KEY,
            position INT,
            rider_name VARCHAR(255),
            team VARCHAR(255),
            time TIME,
            avg_speed FLOAT,
            elevation_gain INT,
            stage_type VARCHAR(255),
            race_name VARCHAR(255)
        )
    """)

    for _, row in df.iterrows():
        if not all([row["Position"], row["rider_name"], row["Team"]]):  # Allow NULL time values
            continue

        row_data = [
            row["Position"], row["rider_name"], row["Team"],
            row["Time"], row["Avg Speed"], row["Elevation Gain"], row["Stage Type"],
            row["Race Name"]
        ]

        cursor.execute("""
            INSERT INTO race_results 
            (position, rider_name, team, time, avg_speed, elevation_gain, stage_type, race_name) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, row_data)

    conn.commit()
    print("✅ Data successfully saved with new race details!")

except mysql.connector.Error as err:
    print(f"❌ MySQL Error: {err}")

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
        print("✅ MySQL connection closed!")
