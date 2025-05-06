# neuralnetworks
## Data Source references
#### Energy Consumption Data Sources:
- https://www.kaggle.com/datasets/twinkle0705/state-wise-power-consumption-in-india
- https://www.eia.gov/opendata/
- https://dataminer2.pjm.com/list
- https://ember-energy.org/data/us-electricity-data-explorer/
#### Climate & Weather Data Sources:
- https://open-meteo.com/en/docs/historical-weather-api
- https://www.ncei.noaa.gov/
- https://cds.climate.copernicus.eu/
- https://www.ncei.noaa.gov/cdo-web/
#### Drive links:
- https://drive.google.com/drive/folders/1aMlG0TlEyeTlrExIoLWOMdqvpSN170X0?usp=sharing
#### Colab link:
- https://colab.research.google.com/drive/1M5NEr3OH-Z-LvmLTwA87sjmBO4albumM?usp=sharing

#### Midterm Presentation:
- https://docs.google.com/presentation/d/1BE-IrNnKVPdXIWZVf5U4Y2PfKNhKmwraaL7ZZbJ_bfU/edit?usp=sharing

#### Notes
- Total energy = Electricity + Natural Gas + Petroleum + Coal
- Estimated electric energy consumption = electric generation * (1 - loss_factor)
- Loss factor = 0.05
- _Assumption: Total Generation value for 1st of each month in the dataset corresponds to that month's totla generated energy_


## Methodology
Early phase:
- Pretrain the model on climate patterns
- Learn feature representations (e.g., seasonal trends, anomalies)
- Build the backbone of a model you’ll later fine-tune with energy data

## Problem Setup
- Predict one climate variable (e.g., temperature_2m_mean) from others
- Learn seasonal/temporal dynamics
- Later: replace the target with energy consumption and fine-tune
