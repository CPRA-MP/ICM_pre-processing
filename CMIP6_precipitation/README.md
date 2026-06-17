# Precipitation Normals and Anomalies (MP29 / 2029 Coastal Master Plan)

This document covers the **monthly precipitation normals** and **projected precipitation anomalies** developed as boundary conditions for the **2029 Coastal Master Plan (CMP)** Integrated Compartment Model.

## Contents

- [1.0 Introduction](#10-introduction)
  - [Objectives](#objectives)
- [2.0 Data and Methods](#20-data-and-methods)
  - [Historic Data Retrieval and Processing](#historic-data-retrieval-and-processing)
  - [Table 1: Stations](#table-1-stations)
  - [Outputs](#outputs)
  - [Table 2: Summary of average regional precipitation](#table-2-summary-of-average-regional-precipitation)
  - [Downscaled Data](#downscaled-data)
  - [Downscaled Data Processing](#downscaled-data-processing)
  - [Precipitation Anomalies](#precipitation-anomalies)
- [3.0 Results](#30-results)
- [Citation](#citation)
- [Acknowledgments](#acknowledgments)

## 1.0 Introduction

To support development of the 2029 Coastal Master Plan (CMP), temperature and precipitation datasets that serve as boundary conditions for the 2029 CMP Integrated Compartment Model were updated.

### Objectives

The first objective was to develop monthly normals for precipitation using historic data. The second objective was to calculate monthly anomalies for precipitation for coastal Louisiana relative to a historical baseline.

## 2.0 Data and Methods

### Historic Data Retrieval and Processing

Monthly summarized precipitation sum data from 1991 to 2020 were downloaded from the National Weather Service ([https://www.weather.gov/wrh/Climate?wfo=lix](https://www.weather.gov/wrh/Climate?wfo=lix)) for stations in coastal Louisiana south of I-10 (Table 1). The New Orleans Audubon site was excluded since it appears to be a heat island and not representative of the coastal wetlands. Monthly precipitation sum values were averaged across all stations by month. Only stations with more than 27 years of data were used for this analysis to obtain a long-term record with sufficient data for characterization. The standard deviation of the monthly mean precipitation values across the dataset (1991 to 2020) was computed.

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

*Figure 1: Mean precipitation sums and mean of the mean precipitation sums for historical coastal Louisiana data (1991-2020).*

> TODO: Add the Figure 1 PNG and link it here. 

### Table 2: Summary of average regional precipitation

**Table 2: Summary of average regional precipitation from historical coastal Louisiana NWS data (1991-2020).**

| Month | CoastalMeanPrecip_mm | Standard Deviation |
|---|---:|---:|
| Jan | 143.58 | 94.14 |
| Feb | 111.65 | 58.51 |
| Mar | 115.83 | 67.89 |
| Apr | 129.68 | 75.50 |
| May | 143.61 | 106.01 |
| Jun | 174.19 | 96.65 |
| Jul | 174.53 | 53.74 |
| Aug | 167.33 | 86.00 |
| Sep | 131.34 | 86.41 |
| Oct | 111.73 | 78.48 |
| Nov | 104.21 | 78.72 |
| Dec | 125.76 | 78.26 |

### Downscaled Data

CMIP6 daily downscaled climate data was downloaded from the NASA NEX-GDDP-CMIP6 catalog ([https://ds.nccs.nasa.gov/thredds/catalog/AMES/NEX/GDDP-CMIP6](https://ds.nccs.nasa.gov/thredds/catalog/AMES/NEX/GDDP-CMIP6)) using NCSS THREDDS Catalog and OPeNDAP. All models available were included, for all scenarios (ssp126, ssp245, ssp370, and ssp585). For each model, scenario, realization, and year, the highest version file was selected, to ensure the latest version was downloaded. Daily data were retrieved for 2025-2099.

### Downscaled Data Processing

pr (precipitation rate) was processed. A coastal Louisiana spatial subset was set with bounding box dimensions lat 28.125 to 31.125, lon 266.125 (0-360 coordinates, −93.875 W) to 272.125 (0-360 coordinates, -87.875 W).

Some of the models use different calendars (365-day Gregorian versus 360-day). The data were subset based on the calendar type, with the 360-day calendar end date of December 30. Daily pr data for each model and scenario were saved as NetCDF4 files.

For each scenario and variable, the daily saved NetCDF4 files were read in. The spatial mean over the grid space was computed for precipitation. Precipitation was converted from kg m^-2 s^-1 to mm/day by multiplying by 86,400. Monthly precipitation totals were calculated by summing mm/day over each month. The models with a non-standard number of days were converted to align with the standard calendar. For each scenario, the ensemble median was computed across all of the models for each month. Results were written to a CSV file.

### Precipitation Anomalies

The historic baseline data file was read in, and the 12 monthly means for precipitation were subtracted from the CMIP-derived precipitation ensemble medians. The values for the anomalies were added to the CSV file.

## Acknowledgments

This document was developed in support of the 2029 Coastal Master Plan under the guidance of the Master Plan Delivery Team (MPDT): 
•	Coastal Protection and Restoration Authority (CPRA) – Ashley Cobb, Jessica Converse, Katie Freer, Elizabeth Jarrell, Valencia Henderson, Sam Martin and Eric White
•	University of New Orleans – Denise Reed 
