# neuralnetworks
## Data Source references
#### Energy Consumption Data Sources:
- https://www.eia.gov/opendata/
- https://dataminer2.pjm.com/list
#### Climate & Weather Data Sources:
- https://open-meteo.com/en/docs/historical-weather-api
- https://www.ncei.noaa.gov/
- https://cds.climate.copernicus.eu/
- https://www.ncei.noaa.gov/cdo-web/
#### Drive links:
- https://drive.google.com/drive/folders/1aMlG0TlEyeTlrExIoLWOMdqvpSN170X0?usp=sharing

#### Notes
Total energy = Electricity + Natural Gas + Petroleum + Coal

## Methodology
Early phase:
- Pretrain the model on climate patterns
- Learn feature representations (e.g., seasonal trends, anomalies)
- Build the backbone of a model you’ll later fine-tune with energy data

## Problem Setup
- Predict one climate variable (e.g., temperature_2m_mean) from others
- Learn seasonal/temporal dynamics
- Later: replace the target with energy consumption and fine-tune
