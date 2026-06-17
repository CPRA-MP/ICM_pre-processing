# Temperature Normals and Anomalies (MP29 / 2029 Coastal Master Plan)

This document covers the **monthly temperature normals** and **projected temperature anomalies** developed as boundary conditions for the **2029 Coastal Master Plan (CMP)** Integrated Compartment Model.


## Contents

- [1.0 Introduction](#10-introduction)
  - [Objectives](#objectives)
- [2.0 Data and Methods](#20-data-and-methods)
  - [Historic Data Retrieval and Processing](#historic-data-retrieval-and-processing)
  - [Table 1: Stations](#table-1-stations)
  - [Outputs](#outputs)
  - [Downscaled Data](#downscaled-data)
  - [Downscaled Data Processing](#downscaled-data-processing)
  - [Temperature Anomalies](#temperature-anomalies)
- [3.0 Results](#30-results)
- [Citation](#citation)
- [Acknowledgments](#acknowledgments)

## 1.0 Introduction

To support development of the 2029 Coastal Master Plan (CMP), temperature and precipitation datasets that serve as boundary conditions for the 2029 CMP Integrated Compartment Model were updated.

### Objectives

The first objective was to develop monthly normals for temperature using historic data. The second objective was to calculate monthly anomalies for temperature for coastal Louisiana relative to a historical baseline.

## 2.0 Data and Methods

### Historic Data Retrieval and Processing

Temperature normals were downloaded from the National Weather Service ([https://www.weather.gov/wrh/Climate?wfo=lix](https://www.weather.gov/wrh/Climate?wfo=lix)) for stations in coastal Louisiana south of I-10 (Table 1), excluding the New Orleans Audubon site since it appears to be a heat island and not representative of the coastal wetlands. Monthly normals are the official NCDC 1991-2020 normals. Units were converted from Fahrenheit to Celsius. For temperature, the mean was computed by month across the average temperature normals for all coastal stations with available data.

Data processing and analysis was implemented using code developed in Python as a reproducible workflow. The Python code is available in the GitHub repository: [https://github.com/CPRA-MP](https://github.com/CPRA-MP).

### Table 1: Stations

**Table 1: Station names and locations of historic precipitation and temperature climate normal data used for analysis (National Weather Service).**

| Station Name | LAT | LON | Precipitation | Temperature |
|---|---|---|:---:|:---:|
| Bayou Sorrel Lock |  |  | Yes | No |
| Chalmette, LA |  |  | No | Yes |
| Donaldsonville 4 SW, LA |  |  | Yes | Yes |
| Galliano, LA |  |  | Yes | Yes |
| Gonzales |  |  | Yes | No |
| New Orleans Intl, LA |  |  | Yes | Yes |
| Ponchatoula 4 E |  |  | Yes | No |
| Slidell |  |  | Yes | Yes |


### Outputs

Temperature data for the five coastal stations show a seasonal cycle with temperatures at a maximum around 27.5 °C in July and August, and a minimum around 12 °C in January (Figure 1).

*Figure 1: Average temperature normals and mean temperature normals across stations for historical coastal Louisiana data (1991-2020).*

> TODO: Add Figure 1 PNG and link it here.

| Month | Mean Temperature in °C |
|---|---:|
| January | 11.54 |
| February | 13.53 |
| March | 16.88 |
| April | 20.37 |
| May | 24.34 |
| June | 27.37 |
| July | 28.20 |
| August | 28.24 |
| September | 26.36 |
| October | 21.66 |
| November | 16.14 |
| December | 12.86 |

*Figure 2: Average temperature normals for coastal stations (1991-2020)*

> TODO: Add Figure 2 PNG and link it here.

### Downscaled Data

CMIP6 daily downscaled climate data was downloaded from the NASA NEX-GDDP-CMIP6 catalog ([https://ds.nccs.nasa.gov/thredds/catalog/AMES/NEX/GDDP-CMIP6](https://ds.nccs.nasa.gov/thredds/catalog/AMES/NEX/GDDP-CMIP6)) using NCSS THREDDS Catalog and OPeNDAP. All models available were included, for all scenarios (ssp126, ssp245, ssp370, and ssp585). For each model, scenario, realization, and year, the highest version file was selected, to ensure the latest version was downloaded. Daily data were retrieved for 2025-2099.

### Downscaled Data Processing

tas (near surface air temperature) was processed. A coastal Louisiana spatial subset was set with bounding box dimensions lat 28.125 to 31.125, lon 266.125 (0-360 coordinates, −93.875 W) to 272.125 (0-360 coordinates, -87.875 W).

Some of the models use different calendars (365-day Gregorian versus 360-day). The data were subset based on the calendar type, with the 360-day calendar end date of December 30. Daily tas data for each model and scenario were saved as NetCDF4 files.

For each scenario and variable, the daily saved NetCDF4 files were read in. The spatial mean over the grid space was computed for temperature. Temperature was converted from Kelvin to Celsius. Temperature data were averaged to monthly means. The models with a non-standard number of days were converted to align with the standard calendar. For each scenario, the ensemble median was computed across all of the models for each month. Results were written to a CSV file.

### Temperature Anomalies

The average monthly historic temperature normals for all coastal Louisiana stations were used to develop temperature anomaly projections for different RCPs. The historic baseline data file was read in, and the 12 monthly means for temperature were subtracted from the CMIP-derived temperature ensemble medians. The values for the anomalies were added to the CSV file.


## Acknowledgments

This document was developed in support of the 2029 Coastal Master Plan under the guidance of the Master Plan Delivery Team (MPDT): 
•	Coastal Protection and Restoration Authority (CPRA) – Ashley Cobb, Jessica Converse, Katie Freer, Elizabeth Jarrell, Valencia Henderson, Sam Martin and Eric White
•	University of New Orleans – Denise Reed 


