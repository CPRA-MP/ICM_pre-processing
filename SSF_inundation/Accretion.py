# Title: Accretion.py
#
# Purpose: Read accretion data and develop rates
#
# Author: Matthew.Vincent@LA.gov
#
# Last Modified: 4/3/2024
###################################################
import pandas as pd
import time
import numpy as np
import math
from datetime import datetime

####CREATE INPUTS AND OUTPUT FRAMEWORKS####
#Read Input Data
file = "U:/MPSubsidence/Reevaluating Subsidence/Task 2 Reanalyzing shallow subsidence/CRMS_DATA/Accretion_Data_03282024.csv" #set input data path
outfile = "U:/MPSubsidence/Reevaluating Subsidence/Task 2 Reanalyzing shallow subsidence/CRMS_DATA/Accretion_Rates.csv" #set output data path
df = pd.read_csv(file) #set data as dataframe

#Devlop Output Framework
CRMSsites = pd.unique(df["Site ID"]) #Determine the unique CRMS stations and their associated IDs
nr = len(CRMSsites) #count number of stations and set that as number of rows for output table
columns = ["Site_ID","Longest Duration (yrs)","Rate (mm/yr)"] #develop headers for output table
nc = len(columns) #Count number of columns for output table
final_df = pd.DataFrame(columns = columns, index = range(nr)) #Create output dataframe

####POPULATE OUTPUT TABLE####
#Station ID
final_df["Site_ID"] = CRMSsites #Populate SiteID
#Split by stations
stations = pd.unique(df["Station ID"]) #create list of unique stations
stationsdf = pd.DataFrame(columns = ["Station_ID","Duration (yrs)","Change (mm)","Rate (mm/yr)"], index = range(len(stations))) #

#Loop through stations to calculate rates
i = 0
for station in stations:
    stationsdf["Station_ID"][i] = station #Identify station being investigated
    subtable = df[(df["Station ID"] == stations[i])] #create a temporary table of all data associated with that station
    last = subtable.index[-1] #determine last sample index
    
    if subtable["Flag is OW"][last] == "NaN":
        last = last-1 #If last sample is flaged move to previous
    if subtable["Mean(Accretion (mm))"][last] == "NaN":
        last = last-1 #if no accretion data in last sample move to previous
    #Duration
    est = pd.to_datetime(subtable["Establishment Date (mm/dd/yyyy)"][last]) #identify establishment date
    finalsamp = pd.to_datetime(subtable["Sample Date (mm/dd/yyyy)"][last]) #identify last sample date
    durat = finalsamp - est #calculate duration
    stationsdf["Duration (yrs)"][i] = durat.days/365.25 #convert duration to decimal years
    #Accretion
    stationsdf["Change (mm)"][i] = subtable["Mean(Accretion (mm))"][last] #identify last accretion measurement
    i=i+1 #move to next station
stationsdf["Rate (mm/yr)"] = stationsdf["Change (mm)"]/stationsdf["Duration (yrs)"] #calculate accretion rate

#Loop through sites and average station rates
t=0
for site in CRMSsites:
    subtable2 = stationsdf[(stationsdf["Station_ID"].str[0:8] == site)] #extract station codes that match the site codes
    
    #Longest Duration
    final_df["Longest Duration (yrs)"][t] = max(subtable2["Duration (yrs)"])
    #Weighted Average Rate
    dursum = sum(subtable2["Duration (yrs)"]) #create variable to help determine time weights
    weights = subtable2["Duration (yrs)"]/dursum #create weights based on duration
    final_df["Rate (mm/yr)"][t] = np.nansum(weights*subtable2["Rate (mm/yr)"]) #average station rates based on time weights
    t=t+1 #move to next site
    
    
final_df.to_csv(outfile) #save output to csv
