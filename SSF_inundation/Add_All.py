# Title: Add_All.py
#
# Purpose: Combine SEC, VA, and location of sites
#
# Author: Matthew.Vincent@LA.gov
#
# Last Modified: 4/4/2024
###################################################
import pandas as pd
import time
import numpy as np
import math
from datetime import datetime

####CREATE INPUTS AND OUTPUT FRAMEWORKS####
#Read Input Data
dadtf = "U:/MPSubsidence/Reevaluating Subsidence/Task 2 Reanalyzing shallow subsidence/CRMS_DATA/Accretion_Rates.csv" #read accretion rates
dsdtf = "U:/MPSubsidence/Reevaluating Subsidence/Task 2 Reanalyzing shallow subsidence/CRMS_DATA/SEC_Rates.csv" #read sec rates
locsf = "U:/MPSubsidence/Reevaluating Subsidence/Task 2 Reanalyzing shallow subsidence/CRMS_DATA/1 km sq centerpoint coords.csv"
outputfile = "U:/MPSubsidence/Reevaluating Subsidence/Task 2 Reanalyzing shallow subsidence/CRMS_DATA/All_combined.csv" #output csv
dadt = pd.read_csv(dadtf)
dsdt = pd.read_csv(dsdtf)
locs = pd.read_csv(locsf)
columns =["Site_ID","VA_Durat","SEC_Durat","VA_Rate","SEC_Rate"]
#final_df = pd.DataFrame(columns = columns, index = range(nr)) #Create output dataframe

#Fix Column names to merge
dadt2 = dadt.rename(columns={"Site_ID" : "SiteID"})
locs2 = locs.rename(columns={"SiteId" : "SiteID"})
dsdt.insert(1,"SiteID",dsdt["Station_ID"].str[0:8])

dadtlocs = pd.merge(dadt2,locs2,on="SiteID",how="outer")
allmerge = pd.merge(dsdt,dadtlocs,on="SiteID",how="outer")
allmerge = allmerge[pd.notna(allmerge["SiteID"])]
allmerge.to_csv(outputfile)
