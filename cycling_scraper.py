import mysql.connector
import pandas as pd

# Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",  # Change if your MySQL user is different
    password="PipeDream_25",  # Replace with your MySQL password
    database="cycling_data"
)

cursor = conn.cursor()

# Read the CSV file containing cycling data
df = pd.read_csv("data/tour_de_france_2023_stage1.csv")

# Convert empty or missing values to None (prevents SQL errors)
df = df.where(pd.notna(df), None)

# Insert data into MySQL
for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO race_results (position, rider, team, time, avg_speed) 
        VALUES (%s, %s, %s, %s, %s)
    """, (row["Position"], row["Rider"], row["Team"], row["Time"], row["Avg Speed"]))

# Commit the transaction
conn.commit()
print("✅ Data successfully saved to MySQL database!")

# Close the connection
cursor.close()
conn.close()
