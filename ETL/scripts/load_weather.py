import os
import time
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

# Initialize Supabase
load_dotenv()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

def load_to_supabase():
    csv_path = "../data/staged/weather_cleaned.csv"
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Missing file: {csv_path}")

    df = pd.read_csv(csv_path)

    # Convert timestamps → strings
    df["time"] = pd.to_datetime(df["time"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
    df["extracted_at"] = pd.to_datetime(df["extracted_at"]).dt.strftime("%Y-%m-%dT%H:%M:%S")

    # Rename CSV columns to match DB
    df.rename(columns={
        "temperature_2m": "temperature_c",
        "relative_humidity_2m": "humidity_percent",
        "windspeed_10m": "wind_speed_kmph"  # make sure DB column matches!
    }, inplace=True)

    # Ensure NaN → None
    df = df.where(pd.notnull(df), None)

    batch_size = 20
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i + batch_size].to_dict("records")
        supabase.table("weather_data").insert(batch).execute()
        print(f"Inserted rows {i+1} → {min(i + batch_size, len(df))}")
        time.sleep(0.3)

    print("Finished loading weather data.")

if __name__ == "__main__":
    load_to_supabase()