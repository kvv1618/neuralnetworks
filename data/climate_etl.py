import requests
import pandas as pd
import time
import json
import os

lat_range = [30, 60]
lon_range = [-120, -70]
start_date = "1995-02-02"
end_date = "2024-02-02"
columns = ['temperature_2m_max', 'temperature_2m_min', 'sunrise', 'sunset', 'daylight_duration', 'sunshine_duration', 'rain_sum', 'showers_sum', 'snowfall_sum', 'precipitation_sum', 'precipitation_probability_max', 'wind_speed_10m_max', 'apparent_temperature_max', 'apparent_temperature_min', 'precipitation_hours', 'et0_fao_evapotranspiration', 'date']
global_df = pd.DataFrame(columns=columns)
for lat in range(lat_range[0], lat_range[1], 10):
    for lon in range(lon_range[0], lon_range[1], 10):
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
        global_df = pd.concat([global_df, df], ignore_index=True)
        time.sleep(60)

        global_df = global_df.drop_duplicates(subset=["date", "lat", "lon"], keep="last")
        global_df.index = pd.to_datetime(global_df["date"], errors="coerce")
        non_numeric_df = global_df.select_dtypes(exclude=["number"]).dropna(axis=1).sort_index().reset_index(drop=True)
        numeric_df = global_df.select_dtypes(include=["number"])
        numeric_df = numeric_df.ffill().bfill().dropna(axis=1, how="all")
        numeric_df = numeric_df.sort_index().interpolate(method="time", limit_direction="both").fillna(numeric_df.mean(numeric_only=True)).dropna(axis=1).reset_index(drop=True)
        global_df = pd.concat([non_numeric_df, numeric_df], axis=1)
        if not global_df.empty:
            global_df.to_csv("climate_data.csv", index=False)
