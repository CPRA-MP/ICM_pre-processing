import pandas as pd
from datetime import datetime
print('Setting up and reading input files.')
# Setup configuration for: future scenario, time period, and output file name
SSP = "ssp2-4.5"
start_year = 2025
end_year = 2079
output_filename_no_leap = f"MP29_future_conditions_AORCprecip_uplandQ_{start_year}_{end_year}_{SSP}_no_leap.csv"
output_filename = f"MP29_future_conditions_AORCprecip_uplandQ_{start_year}_{end_year}_{SSP}.csv"

# this is the conditions matrix developed using the coastal basin averages
conditions_file = "../precip_uplandQ/representative_rainfall_months_2006-2023.csv"
conditions_df = pd.read_csv(conditions_file)

# daily observed data for all compartments
observed_file = "AORC_Precip_UplandQ_Daily_2000_2023.csv"
observed_df = pd.read_csv(observed_file, parse_dates=["yyyy-mm-dd"])

# future conditions based on cmip6 data
future_conditions = "../CMIP6_precipitation/CMIP6_ensemble_median_NWS_anomaly_wetdry_monthly.csv"
future_df = pd.read_csv(future_conditions)

print(f'Building timeseries from monthly wetness classes for {start_year} through {end_year} under: {SSP}')

# add columns for month/year lookups
observed_df["year"] = observed_df["yyyy-mm-dd"].dt.year
observed_df["month"] = observed_df["yyyy-mm-dd"].dt.month

output_blocks = []

start = pd.Timestamp(start_year, 1, 1)
end = pd.Timestamp(end_year, 12, 1)

while start <= end:
    month = start.month
    year = start.year
    if month == 1:
        print(f' - {year}')

    future_conditions_row = future_df.loc[(future_df["month"] == month) & (future_df["year"] == year) & future_df[SSP]]
    # extract the conditions for the particular month/year combo
    condition = future_conditions_row.iloc[0][SSP]

    # lookup the year for historical data extraction based on the month and condition type
    historical_year = conditions_df.loc[conditions_df["month"] == month][condition].values[0]

    # lookup the full month of data from the observed dataset
    # if the month is February, scrub any leap days from the dataset (leap days will be added back in as a last processing step below)
    if month == 2:
        historical_monthly_data = observed_df.loc[
            (observed_df["year"] == historical_year) & (observed_df["month"] == month) ].copy()[0:28]
    else:
        historical_monthly_data = observed_df.loc[
            (observed_df["year"] == historical_year) & (observed_df["month"] == month) ].copy()

    days_in_month = historical_monthly_data["yyyy-mm-dd"].dt.day.values  # array of month days
    # remove leap days from historic datetime list being used as representative data for the month
    if max(days_in_month) == 29:
        days_in_month = days_in_month[0:-1]
    
    # change the timestamps in the monthly data from the historical date to the future date
    historical_monthly_data["yyyy-mm-dd"] = [datetime(year, month, day) for day in days_in_month]

    output_blocks.append(historical_monthly_data.drop(columns=["year", "month"]))

    start = start + pd.DateOffset(months=1)

print(f'Writing output file: {output_filename}')
future_timeseries_df = pd.concat(output_blocks, ignore_index=True)
#future_timeseries_df.to_csv(output_filename_no_leap, index=True, index_label="yyyy-mm-dd")

# address leap years at the end here - add leap days back into daily timeseries and first fill with NA, then interpolate across NA data
future_timeseries_df['yyyy-mm-dd'] = pd.to_datetime(future_timeseries_df['yyyy-mm-dd'])
future_timeseries_df = future_timeseries_df.set_index('yyyy-mm-dd')


full_future_date_range = pd.date_range(start=future_timeseries_df.index.min(), end=future_timeseries_df.index.max(),freq='D')  # full date range including leap years
future_df_reindexed = future_timeseries_df.reindex(full_future_date_range)      # reindex, leap year rows for 2/29 are filled with nan
future_final_df = future_df_reindexed.interpolate(method='linear')              # nans filled with linear interpolation

future_final_df.to_csv(output_filename, index=True, index_label="yyyy-mm-dd")


