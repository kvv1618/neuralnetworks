import requests
import pandas as pd
import os
import numpy as np
from sklearn.preprocessing import StandardScaler
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://api.eia.gov/v2/total-energy/data/"
params = {
    "api_key": os.getenv("EIA_API_KEY"),
    "frequency": "monthly",
    "data[0]": "value",
    "start": "2021-01",
    "end": "2024-01",
    "sort[0][column]": "period",
    "sort[0][direction]": "desc",
    "offset": 0,
    "length": 5000
}

def download_energy_data():
    columns = ["period", "msn", "value"]
    total_df = pd.DataFrame(columns=columns)
    years = set()
    while(len(years) < 3*12):
        response = requests.get(BASE_URL, params=params)
        if response.status_code != 200:
            raise Exception(f"Failed to download data: {response.status_code}")

        data = response.json()
        df = pd.DataFrame(data["response"]["data"])
        if df.empty:
            break
        df = df[columns]
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        df["period"] = pd.to_datetime(df["period"], format="%Y-%m")
        if not df.empty:
            total_df = pd.concat([total_df, df])
        unique_periods = df["period"].unique()
        for period in unique_periods:
            years.add(period.year)
        params["offset"] += params["length"]

    return total_df

df = download_energy_data()
df = df.drop_duplicates(subset=["period", "msn"], keep="last")
df = df.pivot(index="period", columns="msn", values="value")
df.index = pd.to_datetime(df.index, errors="coerce")
df = df.replace([np.inf, -np.inf], np.nan)
df = df.ffill().bfill().dropna(axis=1, how="all")
df = df.sort_index().interpolate(method="time", limit_direction="both").fillna(df.mean())
df.to_csv("energy_data.csv", index=True)


scaler = StandardScaler()
scaled_values = scaler.fit_transform(df.values)
scaled_df = pd.DataFrame(scaled_values, columns=df.columns, index=df.index)

scaled_df.to_csv("scaled_energy_data.csv", index=True)
