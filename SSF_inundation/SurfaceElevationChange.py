# Title: SurfaceElevationChange.py
#
# Purpose: Read surface elevation change data and develop rates
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
file = "U:/MPSubsidence/Reevaluating Subsidence/Task 2 Reanalyzing shallow subsidence/CRMS_DATA/RSET_Data_03282024.csv" #set input data path
outfile = "U:/MPSubsidence/Reevaluating Subsidence/Task 2 Reanalyzing shallow subsidence/CRMS_DATA/SEC_Rates.csv" #set output data path
df = pd.read_csv(file) #set data as dataframe
dfin = df[(df["CMP Team should EXCLUDE"] != 1)] #remove rows to be excluded
dfin = dfin[pd.DataFrame.notna(dfin["Mean(Verified Pin Height (mm))"])] #remove rows with no measurements


#Devlop Output Framework
CRMSsites = pd.unique(dfin["Site ID"]) #Determine the unique CRMS stations and their associated IDs
nr = len(CRMSsites) #count number of stations and set that as number of rows for output table
columns = ["Site_ID","Longest Duration (yrs)","Rate (mm/yr)"] #develop headers for output table
nc = len(columns) #Count number of columns for output table
final_df = pd.DataFrame(columns = columns, index = range(nr)) #Create output dataframe


####POPULATE OUTPUT TABLE####
#Station ID
final_df["Site_ID"] = CRMSsites #Populate SiteID
#Split by stations
stations = pd.unique(dfin["Station ID"]) #create list of stations
stationsdf = pd.DataFrame(columns = ["Station_ID","Duration (yrs)","Establishment Date", "Last Sample Date","Rate (mm/yr)"], index = range(len(stations))) #create station table template

#Loop through stations to calculate rates
i = 0
for station in stations:
    stationsdf["Station_ID"][i] = station #Identify station being investigated
    subtable = dfin[(dfin["Station ID"] == station)] #create a temporary table of all data associated with that station
    dirs = pd.unique(subtable["Direction (Collar Number)"]) #identify the different direction/collars at that station
    collardf = pd.DataFrame(columns = ["Direction", "Est Date", "Final Samp Date", "Durat (yrs)", "Change (mm)", "Rate (mm/yr)"], index = range(len(dirs))) #create a collar/direction table template

    #Loop through the directions to calculate the rates for each direction    
    ii = 0
    for collar in dirs:
        collardf["Direction"][ii] = collar #identify the direction to be analyzed
        subtabledir = subtable[(subtable["Direction (Collar Number)"] == collar)] #create a subtable of the samples associated with each collar
        first = subtabledir.index[0] # Determine the first sample
        last = subtabledir.index[-1] #determine the last sample
            
        #Collar Time
        collardf["Est Date"][ii] = pd.to_datetime(subtabledir["Establishment Date (mm/dd/yyyy)"][last]) #determine collar establishment date
        collardf["Final Samp Date"][ii] = pd.to_datetime(subtabledir["Sample Date (mm/dd/yyyy)"][last]) #determine last sample date along collar
        durat = (collardf["Final Samp Date"][ii] - collardf["Est Date"][ii]) #Determine duration of collar
        collardf["Durat (yrs)"][ii] = durat.days/365.25 #convert duration to decimal years

        #SEC
        finalelev = subtabledir["Mean(Verified Pin Height (mm))"][last] #find last measured pin height
        initelev = subtabledir["Mean(Verified Pin Height (mm))"][first] #find first measured pin height
        collardf["Change (mm)"][ii] = float(finalelev.replace(',','')) - float(initelev.replace(',','')) #calculate change in pin height
        ii = ii+1 #move to next collar
        
    collardf["Rate (mm/yr)"] = collardf["Change (mm)"]/collardf["Durat (yrs)"] #calculate the SEC rate for each collar

    stationsdf["Establishment Date"][i] = min(collardf["Est Date"]) #determine oldest establishment date of station
    stationsdf["Last Sample Date"][i] = max(collardf["Final Samp Date"]) #determine last sample date of station
    stationdurat = stationsdf["Last Sample Date"][i]-stationsdf["Establishment Date"][i] #calculate duration of station
    stationsdf["Duration (yrs)"][i] = stationdurat.days/365.25 #convert duration to decimal years
    weights = collardf["Durat (yrs)"]/sum(collardf["Durat (yrs)"]) #calculate time weights for each collar
    stationsdf["Rate (mm/yr)"][i] = sum(weights*collardf["Rate (mm/yr)"]) #calculate average SEC rate of station
    i=i+1 #move on to next station
    
stationsdf.to_csv(outfile)
