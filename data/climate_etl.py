import requests
import pandas as pd
import time
import json
import os

lat_range = [10, 34]
lon_range = [72, 88]
start_date = "1995-02-02"
end_date = "2024-02-02"
columns = ['temperature_2m_max', 'temperature_2m_min', 'sunrise', 'sunset', 'daylight_duration', 'sunshine_duration', 'rain_sum', 'showers_sum', 'snowfall_sum', 'precipitation_sum', 'precipitation_probability_max', 'wind_speed_10m_max', 'apparent_temperature_max', 'apparent_temperature_min', 'precipitation_hours', 'et0_fao_evapotranspiration', 'date']
df = pd.DataFrame(columns=columns)
for lat in range(lat_range[0], lat_range[1], 2):
    for lon in range(lon_range[0], lon_range[1], 2):
        with open("processed_coordinates.txt", "r") as f:
            if f.read():
                with open("processed_coordinates.txt", "r") as f:
                    processed_coordinates = f.read().splitlines()
                if f"{lat},{lon}" in processed_coordinates:
                    print(f"Skipping lat: {lat}, lon: {lon}")
                    continue
        base_url = f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&start_date={start_date}&end_date={end_date}&daily=temperature_2m_mean,temperature_2m_min,temperature_2m_max,apparent_temperature_mean,apparent_temperature_max,apparent_temperature_min,sunshine_duration,precipitation_sum,rain_sum,snowfall_sum,precipitation_hours"
        while True:
            data = requests.get(base_url)
            if data.status_code != 200:
                print(json.loads(data._content.decode("utf-8"))["reason"])
                if "Daily" in json.loads(data._content.decode("utf-8"))["reason"]:
                    os._exit(0)
                elif "Hourly" in json.loads(data._content.decode("utf-8"))["reason"]:
                    print(f"Rate limit exceeded for lat: {lat}, lon: {lon}. Retrying in 1 hour.")
                    time.sleep(60*60)
                else:
                    time.sleep(60)
            else:
                data_json = data.json()
                break
        if "daily" not in data_json:
            continue
        df = pd.DataFrame(data_json["daily"])
        df["date"] = pd.to_datetime(df["time"])
        df = df.drop(columns=["time"])
        df = df.rename(columns={"latitude": "lat", "longitude": "lon"})
        df["lat"] = lat
        df["lon"] = lon
        print(f"Processing lat: {lat}, lon: {lon}")
        df = pd.concat([df, df], ignore_index=True)
        time.sleep(60)

        df = df.drop_duplicates(subset=["date", "lat", "lon"], keep="last")
        df.index = pd.to_datetime(df["date"], errors="coerce")
        non_numeric_df = df.select_dtypes(exclude=["number"]).dropna(axis=1).sort_index().reset_index(drop=True)
        numeric_df = df.select_dtypes(include=["number"])
        numeric_df = numeric_df.ffill().bfill().dropna(axis=1, how="all")
        numeric_df = numeric_df.sort_index().interpolate(method="time", limit_direction="both").fillna(numeric_df.mean(numeric_only=True)).dropna(axis=1).reset_index(drop=True)
        df = pd.concat([non_numeric_df, numeric_df], axis=1)
        if not df.empty:
            global_df = pd.read_csv("climate_data.csv")
            if global_df.empty:
                global_df = df
            else:
                global_df = pd.concat([global_df, df], ignore_index=True)

            global_df["date"] = pd.to_datetime(global_df["date"], errors="coerce")
            global_df = global_df.dropna(subset=["date"])
            global_df = global_df.drop_duplicates(subset=["date", "lat", "lon"], keep="last")
            global_df = global_df.sort_values(by=["date", "lat", "lon"]).reset_index(drop=True)
            global_df.to_csv("climate_data.csv", index=False)
            with open("processed_coordinates.txt", "a+") as f:
                f.write(f"\n{lat},{lon}")