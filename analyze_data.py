import mysql.connector
import pandas as pd

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="PipeDream_25",
    database="cycling_data",
)

cursor = conn.cursor()

# Read the scraped data (CSV file saved previously)
df = pd.read_csv("data/tour_de_france_2023_stage1.csv")

# Convert empty or missing values to None (prevents SQL errors)
df = df.where(pd.notna(df), None)

# Insert data into MySQL table
for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO race_results (position, rider, team, time, avg_speed) 
        VALUES (%s, %s, %s, %s, %s)
    """, (row["Position"], row["Rider"], row["Team"], row["Time"], row["Avg Speed"]))

# Commit the transaction (saves data to the database)
conn.commit()
print("✅ Data successfully saved to MySQL database!")
