import mysql.connector
import pandas as pd
import numpy as np

# Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",  # Change if your MySQL user is different
    password="PipeDream_25",  # Replace with your MySQL password
    database="cycling_data"
)

cursor = conn.cursor()

# Read the updated CSV file
df = pd.read_csv("data/tour_de_france_2023_stage1.csv")

# ✅ Trim spaces from column names to prevent hidden errors
df.columns = df.columns.str.strip()

# ✅ Convert NaN values to None (so MySQL treats them as NULL)
df = df.replace({np.nan: None})

# ✅ Print preview to verify NaN values are replaced
print("✅ Preview of Data Before Insertion (After NaN Fix):")
print(df.head())

# Insert data into MySQL
for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO race_results (position, rider, team, time, avg_speed, elevation_gain, stage_type, weather_conditions, time_difference) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        row["Position"], row["Rider"], row["Team"], row["Time"], row["Avg Speed"], 
        row["Elevation Gain"], row["Stage Type"], row["Weather Conditions"], row["Time Difference"]
    ))

# Commit the transaction
conn.commit()
print("✅ Data successfully saved with new race details!")

# Close the connection
cursor.close()
conn.close()
