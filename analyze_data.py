import mysql.connector
import pandas as pd
import numpy as np
import os

# ✅ Load credentials from environment variables
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_HOST = os.getenv("MYSQL_HOST")

# ✅ Ensure all environment variables are set
if not all([MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_HOST]):
    raise ValueError("❌ Missing one or more required MySQL environment variables!")

try:
    # ✅ Connect to MySQL
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )
    cursor = conn.cursor()
    print("✅ Successfully connected to MySQL!")

    # ✅ Read the updated CSV file
    df = pd.read_csv("data/tour_de_france_2023_stage1.csv")

    # ✅ Trim spaces from column names to prevent hidden errors
    df.columns = df.columns.str.strip()

    # ✅ Convert numeric columns to proper data types
    df["Position"] = pd.to_numeric(df["Position"], errors="coerce")
    df["Avg Speed"] = pd.to_numeric(df["Avg Speed"], errors="coerce")
    df["Elevation Gain"] = pd.to_numeric(df["Elevation Gain"], errors="coerce")

    # ✅ Convert NaN values explicitly to None (so MySQL treats them as NULL)
    df = df.where(pd.notna(df), None)

    # ✅ Print preview to verify NaN values are replaced
    print("✅ Preview of Data Before Insertion (After NaN Fix):")
    print(df.head())

    # ✅ Insert data into MySQL
    for _, row in df.iterrows():
        try:
            # ✅ Ensure numeric NaN values are converted to None before insertion
            row_data = [
                row["Position"],
                row["Rider"],
                row["Team"],
                row["Time"],
                row["Avg Speed"],
                None if pd.isna(row["Elevation Gain"]) else row["Elevation Gain"],
                row["Stage Type"],
                row["Weather Conditions"],
                row["Time Difference"]
            ]

            print("🔹 Inserting row:", row_data)  # ✅ Print each row before inserting
            
            cursor.execute("""
                INSERT INTO race_results (position, rider, team, time, avg_speed, elevation_gain, stage_type, weather_conditions, time_difference) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, row_data)
        
        except mysql.connector.Error as err:
            print(f"❌ MySQL Error inserting row {row_data}: {err}")

    # ✅ Commit the transaction
    conn.commit()
    print("✅ Data successfully saved with new race details!")

except mysql.connector.Error as err:
    print(f"❌ MySQL Connection Error: {err}")

finally:
    # ✅ Close the connection
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
