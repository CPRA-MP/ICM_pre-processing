#Originally created on Jan 28 2021
#    last modified on April 6 2026
#@author: madelinel & ewhite12

# This script is designed to build daily flow timeseries of flow diversions off of the Mississippi River, including the distributary network in the Birdsfoot Delta.
# input/output flowrates are saved in cubic meters per second (cms), but all operational rules are defined in cubic feet per second (cfs)
#
# This script was updated for MP29 to build all files (including suspended fines and sands files from processed USGS observational data and assumed future scenario wetness classes.
#
# The algorithm structure is as follows:
#       1 - read input filenames and working directories - most of the information is going to be included in the input file: TribsListFile = 'MP2029_TribQ_columns.csv'
#       2 - the first processing step will be to compile the individual observed flow records (for all Type 1 tributaries that have an observed record file) into a single file
#       3 - this single tributary flow record will be used to calculate diversion flowrates based on operational rule curves
#           - for diversions with operational rule curves, there also needs to be an implementation year in which the diversion is activated
#           - if the diversion/crevasse is historic, the implementation year is 1900, otherwise the implementation year is the calendar year in which the diversion/crevasse becomes active
#           - if the diversion/crevasse is not active in the current simulation, the implementation year should be set to 9999
#       4 - once the daily flows for the observed years range (set in 'obs_years') are calculated for all tributaries and diversions, these flows are saved to file: 'TribQ_observed_out_file'
#       5 - the daily flows from the observation period are used to calculate suspended sediment concentrations (fines & sands) for the observation period - these are saved to files: 'TribF_observed_out_file' & 'TribS_observed_out_file'
#       6 - the three files from the observed period are used to piece together the future scenarios timeseries using the wetness classes from future scenario data and the representative periods for each wetness class


import numpy as np
from datetime import datetime as dt
import matplotlib.pyplot as plt

obs_years = range(2006,2025)
fut_years = range(2025,2080)
scenario = 'ssp2-4.5'
TribListFile = 'MP2029_TribQ_columns.csv'

process_observed = True
process_sediment = False

ObsQ_dir = '../observed_tributary_flows/Data_Filled'
ObsQ_all_file = 'MP29_S00_G000_%04d_%04d_obsQ.csv' % (obs_years[0],obs_years[-1])
TribQ_observed_out_file = 'MP29_S00_G000_%04d_%04d_TribQ.csv' % (obs_years[0],obs_years[-1])
TribF_observed_out_file = 'MP29_S00_G000_%04d_%04d_TribF.csv' % (obs_years[0],obs_years[-1])
TribS_observed_out_file = 'MP29_S00_G000_%04d_%04d_TribS.csv' % (obs_years[0],obs_years[-1])

TribQ_future_all_file = 'MP29_%s_future_conditions_tribQ_%04d_%04d_no_diversions_GFDL_ESM2G_MissAtch.csv'  % (scenario,fut_years[0],fut_years[-1])
TribQ_future_out_file = 'MP29_%s_%04d_%04d_TribQ.csv' % (scenario,fut_years[0],fut_years[-1])
TribF_future_out_file = 'MP29_%s_%04d_%04d_TribF.csv' % (scenario,fut_years[0],fut_years[-1])
TribS_future_out_file =  'MP29_%s_%04d_%04d_TribS.csv' % (scenario,fut_years[0],fut_years[-1])


# read in tributary list
print('reading in tributary attributes from file')
tribs_col = np.genfromtxt(TribListFile,usecols=0,skip_header=1,delimiter=',',dtype='int')
tribs = np.genfromtxt(TribListFile,usecols=1,skip_header=1,delimiter=',',dtype='str')
tribs_files = np.genfromtxt(TribListFile,usecols=9,skip_header=1,delimiter=',',dtype='str')
div_vars = np.genfromtxt(TribListFile,usecols=6,skip_header=1,delimiter=',',dtype='str')
div_impl_yrs = np.genfromtxt(TribListFile,usecols=7,skip_header=1,delimiter=',',dtype='int')

# read in data from input file into arrays that will be converted to dictionaries with tribcol as key
tribs_types_arr = np.genfromtxt(TribListFile,usecols=2,skip_header=1,delimiter=',',dtype='int')
sand_types_arr = np.genfromtxt(TribListFile,usecols=4,skip_header=1,delimiter=',',dtype='int')
fine_types_arr = np.genfromtxt(TribListFile,usecols=5,skip_header=1,delimiter=',',dtype='int')

TSS_qmaxsands_arr = np.genfromtxt(TribListFile,usecols=10,skip_header=1,delimiter=',',dtype='float')
TSS_max_sand_portions_arr = np.genfromtxt(TribListFile,usecols=11,skip_header=1,delimiter=',',dtype='float')
TSS_trib_areas_arr = np.genfromtxt(TribListFile,usecols=12,skip_header=1,delimiter=',',dtype='float')


# convert arrays read in from file into dictionaries with tribcol as key
tribs_types = {}
sand_types = {}
fine_types = {}
TSS_trib_areas = {}
TSS_qmaxsands = {}
TSS_max_sand_portions = {}

for n in range(0,len(tribs_col)):
    tribs_types[tribs_col[n]] = tribs_types_arr[n]                      # integer storing tributary type id
    sand_types[tribs_col[n]] = sand_types_arr[n]                        # integer storing sand rating curve type id
    fine_types[tribs_col[n]] = fine_types_arr[n]                        # integer storing fines rating curve type id
    TSS_trib_areas[tribs_col[n]] = TSS_trib_areas_arr[n]                # float storing the tributary area upstream of gage used for TSS rating curves for Florida Parishes tributaries with limited TSS data (see MP23 Appendix B2, section 5.5)
    TSS_qmaxsands[tribs_col[n]] = TSS_qmaxsands_arr[n]                  # float storing flowrate (cms) used to define the maximum flow where peak sand suspension occurs - used to partition TSS into sands and fines (see MP23 Appendix B2, section 5.5)
    TSS_max_sand_portions[tribs_col[n]] = TSS_max_sand_portions_arr[n]  # float storing maximum portion of TSS that can is sand (derived from Miss. River data) - used to partition TSS into sands and fines (see MP23 Appendix B2, section 5.5)

nTributaries_null = 0              # number of riverine input timeseries that are no longer used in and set to zero values in in TribQ, TribF, TribS, and QMult
nTributaries = 0                   # number of riverine input timeseries with observed flowrates
nTributaries_calc = 0              # number of riverine input timeseries that are calculated from other observed flows
nMissRiv_Diversions = 0            # number of Mississippi River diversion timeseries included in TribQ, TribF, TribS, and QMult
nBFD_Passes = 0                    # number of distributary passes timeseries in the BFD included in TribQ, TribF, TribS, and QMult
nAtchRiv_Diversions = 0            # number of Atchafalaya River diversion timeseries included in TribQ, TribF, TribS, and QMult

for tt in tribs_types.values():
    if tt == 0:
        nTributaries_null += 1
    if tt == 1:
        nTributaries += 1
    if tt == 2:
        nTributaries_calc += 1
    if tt == 3:
        nMissRiv_Diversions += 1
    if tt == 4:
        nBFD_Passes += 1
    if tt == 5:
        nAtchRiv_Diversions += 1        
nTribs = nTributaries_null + nTributaries + nTributaries_calc + nMissRiv_Diversions + nBFD_Passes + nAtchRiv_Diversions # total number of timeseries read in as tributary boundary conditions in TribQ

# set some empty dictionaries used to format observed data
implementation = {}
obsQ = {}
obsQ_structured = {}

if process_observed == False:
    for nf in range(0,nTribs):
        col = tribs_col[nf]
        trib = tribs[nf]
        file = tribs_files[nf]
        divnm = div_vars[nf]
        impyr = div_impl_yrs[nf]
        typ = tribs_types[col]
        implementation[divnm] = impyr 
else:
    print('reading in observed tributary flow from:')
    for nf in range(0,nTribs):
        col = tribs_col[nf]
        trib = tribs[nf]
        file = tribs_files[nf]
        divnm = div_vars[nf]
        impyr = div_impl_yrs[nf]
        typ = tribs_types[col]
        
        f = '%s/%s' % (ObsQ_dir,file)
        if typ in [1]:
            print('   - %s' % file)
            obsQ[col] = np.genfromtxt(f,delimiter=',',skip_header=1,usecols=[0,1],dtype='str')
        #if typ in [2]:
        
        #  read in implementation year
    
        implementation[divnm] = impyr    
    
    # prepare dictionary key that is all daily dates
    print('restructuring observed data from multiple files')
    for row in obsQ[1]:
        d = dt.strptime(row[0],'%Y-%m-%d')
        if d.year in obs_years:
            obsQ_structured[d] = {}
    
    # restructure observed tributary data for output file (only includes data for trib_type = 1)
    for tribcol in obsQ.keys():
        for row in obsQ[tribcol]:
            d = row[0]
            q = row[1]
            dk = dt.strptime(d,'%Y-%m-%d')
            if dk.year in obs_years:
                try:
                    obsQ_structured[dk][tribcol] = float(q)
                except:
                    obsQ_structured[dk][tribcol] = 0.0
    
    for tribcol in tribs_col:
        typ = tribs_types[tribcol]
        if typ != 1:
            for dk in obsQ_structured.keys():
                if typ == 0:
                    obsQ_structured[dk][tribcol] = 0.0
                elif typ == 2:
                    obsQ_structured[dk][tribcol] = 0.0
                else:
                    obsQ_structured[dk][tribcol] = 0.0
    
    print('writing all daily flows into single file:  %s' % ObsQ_all_file)
    with open(ObsQ_all_file,mode='w') as obsQ_out:
        # write header line to structured obsQ outfile
        line = 'newline'
        for tribcol in tribs_col:
            if line == 'newline':
                line = tribcol
            else:
                line = '%s,%s' % (line,tribcol)
        obsQ_out.write('%s\n' % line)          
    
        # write daily outputs to structured obsQ outfile
        for d in obsQ_structured.keys():
            line = 'newline'
            for tribcol in tribs_col:
    ##            if tribs_types[tribcol] == 1:
    ##                qout = obsQ_structured[d][tribcol]
    ##            else:
    ##                qout = 0.0
                qout = obsQ_structured[d][tribcol]
                if line == 'newline':
                    line = '%0.4f' % qout
                else:
                    line = '%s,%0.4f' % (line,qout)
            obsQ_out.write('%s,! %04d-%02d-%02d\n' % (line,d.year,d.month,d.day))
    

print('calculating diversion flows based on operational rating curves')
trib_cols   = range(0,nTributaries+nTributaries_null+nTributaries_calc)
TribQ_in_date_col    = [-1]         # last column of input TribQ.csv is the date
MissRiv_col = 10                    # column 11 of TribQ.csv is the Miss. River @ Tarbert Landing data

if process_observed == True:
    TribQ_in    = np.genfromtxt(ObsQ_all_file,delimiter=',',dtype=str,skip_header=1,usecols=trib_cols)
    dates_all   = np.genfromtxt(ObsQ_all_file,delimiter=',',dtype=str,skip_header=1,usecols=TribQ_in_date_col)

else:
    TribQ_in    = np.genfromtxt(TribQ_future_all_file,delimiter=',',dtype=str,skip_header=1,usecols=trib_cols)
    dates_all   = np.genfromtxt(TribQ_future_all_file,delimiter=',',dtype=str,skip_header=1,usecols=TribQ_in_date_col)

date_comment = False
try:
    dates_all = [da.split()[1] for da in dates_all]                                                # this will work if date is formatted as '! YYYYMMDD' or '! YYYY-MM-DD'
    date_comment = True
except:
    print('dates do not have leading ! ')
    
    
# read in Mississippi River @ Tarbert Landing (input data is in cms)
MissTarb_cms = [ float(q) for q in TribQ_in[:,[MissRiv_col]] ]
MissTarb_cfs = [ q/(0.3048**3.0) for q in MissTarb_cms ]

# read in date timeseries
ndays = len(dates_all)

# build zero arrays for each diversion timeseries
Atch_cfs = np.zeros(ndays)      # Atchafalaya River
Atch_cms = np.zeros(ndays)      
Morg_cfs = np.zeros(ndays)      # Morganza Floodway
Morg_cms = np.zeros(ndays)
IAFT_cfs = np.zeros(ndays)      # Increase Atchafalaya Flows to Terrebonne
IAFT_cms = np.zeros(ndays)
AtRD_cfs = np.zeros(ndays)      # Atchafalaya River Diversion
AtRD_cms = np.zeros(ndays)
BLaF_cfs = np.zeros(ndays)      # Bayou Lafourche Diversion
BLaF_cms = np.zeros(ndays)
FDWB_cfs = np.zeros(ndays)      # Freshwater Delivery to Westeran Barataria
FDWB_cms = np.zeros(ndays)
UBaH_cfs = np.zeros(ndays)      # Upper Barataria Hydrologic Restoration
UBaH_cms = np.zeros(ndays)
UFWD_cfs = np.zeros(ndays)      # Union Freshwater Diversion
UFWD_cms = np.zeros(ndays)
WMPD_cfs = np.zeros(ndays)      # West Maurepas Sediment Diversion
WMPD_cms = np.zeros(ndays)
MSRM_cfs = np.zeros(ndays)      # Mississippi River Reintroduction in Maurepas Swamp
MSRM_cms = np.zeros(ndays)
EdDI_cfs = np.zeros(ndays)      # Edgard Diversion
EdDI_cms = np.zeros(ndays)
Bonn_cfs = np.zeros(ndays)      # Bonnet Carre
Bonn_cms = np.zeros(ndays)
MLBD_cfs = np.zeros(ndays)      # Manchac Landbridge Diversion (timeseries not used - implemented via links)
MLBD_cms = np.zeros(ndays)
LaBr_cfs = np.zeros(ndays)      # LaBranche Hydrological Restoration
LaBr_cms = np.zeros(ndays)
LaBD_cfs = np.zeros(ndays)      # LaBranche Diversion (timeseries not used - implemented via links)
LaBD_cms = np.zeros(ndays)
DavP_cfs = np.zeros(ndays)      # Davis Pond
DavP_cms = np.zeros(ndays)
AmaD_cfs = np.zeros(ndays)      # Ama Sediment Diversion
AmaD_cms = np.zeros(ndays)
IHNC_cfs = np.zeros(ndays)      # Inner Harbor Navigational Canal
IHNC_cms = np.zeros(ndays) 
CWDI_cfs = np.zeros(ndays)      # Central Wetlands Diversion
CWDI_cms = np.zeros(ndays)
Caer_cfs = np.zeros(ndays)      # Caernarvon
Caer_cms = np.zeros(ndays)
UBrD_cfs = np.zeros(ndays)      # Upper Breton Diversion
UBrD_cms = np.zeros(ndays)  
MBrD_cfs = np.zeros(ndays)      # Mid-Breton Sound Diversion
MBrD_cms = np.zeros(ndays)
Naom_cfs = np.zeros(ndays)      # Naomi
Naom_cms = np.zeros(ndays)
MBaD_cfs = np.zeros(ndays)      # Mid-Barataria Diversion
MBaD_cms = np.zeros(ndays) 
WPLH_cfs = np.zeros(ndays)      # West Point a la Hache
WPLH_cms = np.zeros(ndays) 
LPlq_cfs = np.zeros(ndays)      # Lower Plaquemines River Sediment Plan
LPlq_cms = np.zeros(ndays) 
LBaD_cfs = np.zeros(ndays)      # Lower Barataria Diversion
LBaD_cms = np.zeros(ndays) 
LBrD_cfs = np.zeros(ndays)      # Lower Breton Diversion
LBrD_cms = np.zeros(ndays) 

# build zero arrays for each Mississippi River Distributary Pass
MGPS_cfs = np.zeros(ndays)      # Mardi Gras Pass
MGPS_cms = np.zeros(ndays)  
Bohe_cfs = np.zeros(ndays)      # Bohemia
Bohe_cms = np.zeros(ndays) 
Ostr_cfs = np.zeros(ndays)      # Ostrica
Ostr_cms = np.zeros(ndays)  
FStP_cfs = np.zeros(ndays)      # Ft. St. Philip 
FStP_cms = np.zeros(ndays)
Bapt_cfs = np.zeros(ndays)      # Baptiste Collette
Bapt_cms = np.zeros(ndays) 
GrPa_cfs = np.zeros(ndays)      # Grand Pass
GrPa_cms = np.zeros(ndays) 
WBay_cfs = np.zeros(ndays)      # West Bay
WBay_cms = np.zeros(ndays) 
SCut_cfs = np.zeros(ndays)      # SmallCuts
SCut_cms = np.zeros(ndays) 
CGap_cfs = np.zeros(ndays)      # Cubit's Gap
CGap_cms = np.zeros(ndays) 
SWPS_cfs = np.zeros(ndays)      # SW Pass Ratings Curve
SWPS_cms = np.zeros(ndays) 
SPas_cfs = np.zeros(ndays)      # S Pass
SPas_cms = np.zeros(ndays)  
PLou_cfs = np.zeros(ndays)      # Pass a Loutre
PLou_cms = np.zeros(ndays)  
SWPR_cfs = np.zeros(ndays)      # SW Pass Residual
SWPR_cms = np.zeros(ndays)  

for d in range(0,ndays):
    
    date = dates_all[d]
    yr = int(date[0:4])                                                   # this will work if date is formatted as YYYYMMDD, or YYYY-MM-DD, etc.
    if date_comment == True:
        month = int(date[4:6])                                                # this will work if date is formatted as YYYYMMDD
    else:
        month = int(date[5:7])                                                # this will work if date is formatted as YYYY-MM-DD
    # month, yr = int(date.split('/')[0]), int(date.split('/')[2])        # this will work if date is formatted as MM/DD/YYYY
    # year = date.split('-')[2]                                          # this will work if date is formatted as MM-DD-YYYY
    
    Qresidual   = MissTarb_cfs[d]       # this residual flow is the flow in the Mississippi River that is continuously updated as flows are diverted out of the river
    
    ##########################################
    ###   Atchafalaya River @ Simmesport   ###
    ##########################################
    # input dataset is Mississippi River flow at Tarbert Landing
    # Tarbert Landing is located downstream of the Old River Control Structure
    # Assume 70/30 flow split at Old River Control Structure (river mile 316)
    # 70% of Mississippi River flow is kept in main channel (this is the observed discharge at Tarbert Landing)
    # 30% of Mississippi River flow is diverted into Atchafalaya River
    
    Q_ORCS = MissTarb_cfs[d]/0.7
    Qdiv = Q_ORCS*0.3
    
    Atch_cfs[d] = Qdiv
    Atch_cms[d] = Qdiv*(0.3048**3)
        
    Q_Atch_Simm = Qdiv
        
    ##############################
    ###   Morganza Floodway    ###
    ##############################
    # river mile 280
    # not active, set to zero (this is redundant since Morg_cfs is already set as a zero array above)
    
    Qdiv = 0
    Morg_cfs[d] = Qdiv
    Morg_cms[d] = Qdiv*(0.3048**3)
    
    #####################################################
    ###   Atchafalaya River at Morgan City (rating)   ###
    #####################################################
    
    Q_Atch_MorganCity = 0.70*Q_Atch_Simm - 42040.0      # rating curve developed from G500 simulation by Moffat & Nichol
    Qresidual_Atch = Q_Atch_MorganCity
    
    ###################################################
    ###   Increase Atchafalaya Flows to Terrebonne  ###
    ###################################################
    # location at GIWW
    # rating curve needs to be a function of Atchafalaya River @ Simmesport since it is calculated here from the flows directly downstream of Old River Control Structure
    # MP2023: project 139
    #    Dredging of the Gulf Intracoastal Waterway (GIWW) and construction of a bypass structure at the Bayou Boeuf Lock from the Atchafalaya River 
    #    to Terrebonne marshes allowing peak flows of approximately 20,000 cfs 
    #
    # Rating curve for TE-110 30% design used a stage rating curve at Morgan City
    # Converting the stage curve to a discharge rating curve resulted in:
    #       diversion (cfs) = 0.14*Q_MorganCity(cfs) - 1,880
    #
    # the diversion is deactivated during the spring flood,
    # which corresponds to a flow threshold of 250,000 cfs at Morgan City (417,000 cfs at Simmesport)

    impl_yr = implementation['IAFT']
    if yr <= impl_yr:
        Qdiv = 0
    else:   
        if Q_Atch_MorganCity >= 250000:
            Qdiv = 0
        else:
            Qdiv = max(0,min(0.14*Q_Atch_MorganCity-1880.0, 30000))
            
    IAFT_cfs[d] = Qdiv
    IAFT_cms[d] = Qdiv*(0.3048**3)
    Qresidual_Atch -= Qdiv
    
    
    ########################################
    ###   Atchafalaya River Diversion    ###
    ########################################
    # location south of GIWW 
    # rating curve needs to be a function of Atchafalaya River @ Simmesport since it is calculated here from the flows directly downstream of Old River Control Structure
    # MP2023: project 108
    #    30,000 cfs capacity (modeled at 26% of the Atchafalaya River flow upstream of the confluence with Bayou Shaffer)
    
    impl_yr = implementation['AtRD']
    if yr <= impl_yr:
        Qdiv = 0
    else:   
        if Qresidual_Atch <= 0:
            Qdiv = 0
        else:
            Qdiv = min(0.26*Qresidual_Atch, 30000)
            
    AtRD_cfs[d] = Qdiv
    AtRD_cms[d] = Qdiv*(0.3048**3)
    Qresidual_Atch -= Qdiv

    
    #####################################################
    ###          Bayou Lafourche Diversion            ###
    #####################################################
    # river mile 176
    # current condition is 500 cfs but an additional 1000 cfs pump is in the permitting stage as of 4/27/2021
    # Constant diversion flow of 1,500 cfs
    impl_yr  = implementation['BLaF']

    if yr <= impl_yr:
        Qdiv = 0
    else:   
        if Qresidual >= 1500:
            Qdiv = 1500
        else:
            Qdiv = Qresidual
    # update Bayou LaFourche array with diverted volumes    

    BLaF_cfs[d] = Qdiv
    BLaF_cms[d] = Qdiv*(0.3048**3)
    Qresidual -= Qdiv

    #####################################################
    ###    Freshwater Delivery to Western Barataria   ###
    #####################################################
    # river mile 176
    # Freshwater Delivery to Western Barataria
    # MP2023: project 322
    # add additional 500 cfs capacity to Bayou Lafourche pump

    impl_yr = implementation['FDWB']

    if yr <= impl_yr:
        Qdiv = 0
    else:   
        if Qresidual >= 500:
            Qdiv += 500
        else:
            Qdiv += Qresidual
       
    FDWB_cfs[d] = Qdiv
    FDWB_cms[d] = Qdiv*(0.3048**3)
    Qresidual -= Qdiv

    #####################################################
    ###    Upper Barataria Hydrologic Restoration     ###
    #####################################################
    # Upper Barataria Hydrologic Restoration
    # MP2023: project 324
    #  Construction of a 750 cfs pump/siphon structure along Bayou Lafourche to supply freshwater into the marshes, bayous, and lakes of the Upper Barataria Sub-Basin
    # pump 750 cfs into Bayou Lafourche to be routed down BLaF and eventually eastward into Upper Barataria
    # add this diversion flow to the pre-existing Bayou Lafourche flow calculated above

    impl_yr = implementation['UBaH']

    if yr <= impl_yr:
        Qdiv = 0
    else:   
        if Qresidual >= 750:
            Qdiv += 750
        else:
            Qdiv += Qresidual

    UBaH_cfs[d] = Qdiv
    UBaH_cms[d] = Qdiv*(0.3048**3)
    Qresidual -= Qdiv

    
    #######################################
    ###   Union Freshwater Diversion    ###
    #######################################
    # river mile 169
    # No diversion flow below 200,000 or above 600,000, Diversion flow of 25,000 between 400,000 and 600,000, Else, diversion flow = 0.125x-25000
    # MP2023: project 244
    #     modeled at 25,000 cfs when Mississippi River flow equals 400,000 cfs; 
    #     closed when river flow is below 200,000 cfs or above 600,000 cfs; 
    #     a variable flow rate calculated using a linear function from 0 to 25,000 cfs for river flow between 200,000 cfs and 400,000 cfs 
    #     and held constant at 25,000 cfs for river flow between 400,000 cfs and 600,000 cfs
    
    impl_yr = implementation['UFWD']
    
    if yr <= impl_yr:
        Qdiv = 0
    else:
        if Qresidual < 200000 or Qresidual >= 600000:
            Qdiv = 0
        elif Qresidual > 400000 and Qresidual < 600000:
            Qdiv = 25000
        else:
            Qdiv = 0.125*Qresidual - 25000
        
    UFWD_cfs[d] = Qdiv  
    UFWD_cms[d] = Qdiv*(0.3048**3)
    Qresidual -= Qdiv
     
    
    #############################################
    ###   West Maurepas Sediment Diversion    ###
    #############################################
    # river mile 169
    # Diversion flow of 3,000 cfs
    # MP2023: project 305
    #     modeled at 50,000 cfs when the Mississippi River flow equals 1,000,000 cfs; 
    #     open with a variable flow rate calculated using a linear function from 0 to 50,000 cfs for river flow between 200,000 cfs and 1,000,000 cfs;
    #     constant flow rate of 50,000 cfs for river flow above 1,000,000 cfs. No operation below 200,000 cfs
    # Note that this is different than the West Maurepas Diversion from the 2012 and 2017 Master Plans (see below)

    impl_yr = implementation['WMPD']
    
    if yr <= impl_yr:
        Qdiv = 0
    else:
        if Qresidual < 200000:
            Qdiv = 0
        else:
            Qdiv = min(0.0625*Qresidual-12500, 50000)
       
    WMPD_cfs[d] = Qdiv
    WMPD_cms[d] = Qdiv*(0.3048**3)
    Qresidual -= Qdiv


# not used #    #####################################
# not used #    ###   West Maurepas Diversion     ###
# not used #    ###  legacy from 2012 and 2017 MPs ##
# not used #    #####################################
# not used #    # river mile 162
# not used #    # Diversion flow of 3,000 cfs
# not used #
# not used #    impl_yr = implementation['WMPD']
# not used #    
# not used #    if yr <= impl_yr:
# not used #        Qdiv = 0
# not used #    else:
# not used #        if Qresidual >= 3000:
# not used #            Qdiv = 3000
# not used #        else:
# not used #            Qdiv = Qresidual
# not used #      
# not used #    WMPD_cfs[d] = Qdiv
# not used #    WMPD_cms[d] = Qdiv*(0.3048**3)
# not used #    Qresidual -= Qdiv

    #################################################################
    ###   Mississippi River Reintroduction into Maurepas Swamp    ###
    #################################################################
    # river mile 144
    # Minimum operation in April and July-December
    # January-March and May-June operation follows the rating curve 2466.1*ln(Qresidual)-21462 with a maximum of 2,000 cfs
        
    impl_yr = implementation['MSRM']
    
    if yr <= impl_yr:
        Qdiv = 0
    else:
        if month == 4:
            Qdiv = 10
        elif month >= 7:
            Qdiv = 10
        else:
            Qdiv = max(10, (min(2000, 2466.1*np.log(Qresidual)-21462)))
        
    MSRM_cfs[d] = Qdiv  
    MSRM_cms[d] = Qdiv*(0.3048**3)
    Qresidual -= Qdiv
    

    #############################
    ###   Edgard Diversion    ###
    #############################
    # river mile 137
    # off below 200,000; rating curve of 0.0625x-12500 between 200,000 and 600,000; constant flow of 25,000 cfs at 600,000; off between 600,000 and 1,250,000; constant flow of 35,000 cfs above 1,250,000
    # MP2023: project 323
    #    modeled at 25,000 cfs when Mississippi River flow equals 600,000 cfs; 
    #    open with a variable flow rate calculated using a linear function from 0 to 25,000 cfs for river flow between 200,000 cfs and 600,000 cfs; 
    #    no flow between 600,000 cfs and 1,250,000 cfs; constand flow rate of 35,000 cfs when river is above 1,250,000 cfs
    
    impl_yr = implementation['EdDI']
    
    if yr <= impl_yr:
        Qdiv = 0
    else:
        if Qresidual == 600000:
            Qdiv = 25000
        elif Qresidual < 200000 or Qresidual > 600000 and Qresidual < 1250000:
            Qdiv = 0
        elif Qresidual >= 1250000:
            Qdiv = 35000
        else:
            Qdiv = 0.0625*Qresidual - 12500
        
    EdDI_cfs[d] = Qdiv  
    EdDI_cms[d] = Qdiv*(0.3048**3)
    Qresidual -= Qdiv        
        
    
    ##################################
    ###   Bonnet Carre Spillway    ###
    ##################################
    # river mile 128
    # River flow in excess of 1,250,000 cfs is diverted
         
    impl_yr = implementation['Bonn']
    
    if yr <= impl_yr:
        Qdiv = 0
    else:
        if Qresidual >= 1250000:
            Qdiv = Qresidual - 1250000
        else:
            Qdiv = 0
        
    Bonn_cfs[d] = Qdiv 
    Bonn_cms[d] = Qdiv*(0.3048**3)
    Qresidual -= Qdiv
    
    
    #########################################
    ###   Manchac Landbridge Diversion    ###
    #########################################
    # IMPLEMENTED VIA LINKS FOR  RUNS DO NOT USE THIS RATING CURVE
    # INSTEAD IMPLEMENT A NEW STATIC LINK WITH CAPACITY APPROXIMATELY EQUAL TO PEAK DIVERTED DISCHARGE
    #
    # from Bonnet Carre, which is at river mile 128
    # Diversion flow of 2,000 cfs at Bonnet Carre flow above 2,000 cfs *2017 MP had a diverted rate of 5,000 cfs*
    # MP2023: project 242
    #    A structure in the existing western spillway guide levee with a capacity of 2,000 cfs to increase freshwater exchange with adjacent wetlands 
        
    impl_yr = implementation['MLBD']
    
    if yr <= impl_yr:
        Qdiv = 0
    else:
        if Bonn_cfs[d] >= 2000:
            Qdiv = 2000
        else:
            Qdiv = Bonn_cfs[d]
           
    MLBD_cfs[d] = Qdiv
    MLBD_cms[d] = Qdiv*(0.3048**3)

    
    #########################################
    ###   LaBranche Diversion             ###
    #########################################
    # IMPLEMENTED VIA LINKS FOR  RUNS DO NOT USE THIS RATING CURVE
    # INSTEAD IMPLEMENT A NEW STATIC LINK WITH CAPACITY APPROXIMATELY EQUAL TO PEAK DIVERTED DISCHARGE
    #
    # from Bonnet Carre, which is at river mile 128
    # MP2023: project 304
    #    Modeled at 850 cfs when Bonnet Carre is at 10,000 cfs increasing linearly to 17,500 cfs 
    #    when Bonnet Carre is at 250,000 cfs
    #    
    #impl_yr = implementation['LaBD']
    #
    #if yr <= impl_yr:
    #    Qdiv = 0
    #else:
    #    if Bonn_cfs[d] < 10000:
    #       Qdiv = 0
    #    else:
    #        Qdiv = min(0.069375*Bonn_cfs[d]+156.25, 175000)
           
    #LaBD_cfs[d] = Qdiv
    #LaBD_cms[d] = Qdiv*(0.3048**3)

    
    ############################################
    ###   Davis Pond Freshwater Diversion    ###
    ############################################
    # river mile 118
    # Diversion flow of rating curve 1269.1454*ln(Qresidual)-9932.94805 with a maximum of 10,594 cfs
          
    impl_yr = implementation['DavP']
    
    if yr <= impl_yr:
        Qdiv = 0
    else:
        Qdiv = min(max(0,1269.1454*np.log(Qresidual*0.3048**3) - 9932.94805), 10594.3487)
        
    DavP_cfs[d] = Qdiv  
    DavP_cms[d] = Qdiv*(0.3048**3)
    Qresidual -= Qdiv
    

    ###############################################
    ###   LaBranche Hydrologic Restoration    ###
    ###############################################
    # river mile 116
    # Diversion flow of 750 cfs at river flows above 750 cfs
    # MP2023: project 245
    #     Construction of a pump/siphon with a constant flow of 750 cfs into the LaBranche wetlands via the Mississippi River to restore the historically fresh to intermediate marshes
        
    impl_yr = implementation['LaBr']
    
    if yr <= impl_yr:
        Qdiv = 0
    else:
        if Qresidual >= 750:
            Qdiv = 750
        else:
            Qdiv = Qresidual
        
    LaBr_cfs[d] = Qdiv  
    LaBr_cms[d] = Qdiv*(0.3048**3)
    Qresidual -= Qdiv
        
        
    ###################################
    ###   Ama Sediment Diversion    ###
    ###################################
    # river mile 115
    # Diversion flow of rating curve 0.0625*Qresidual-12500 at river flows above 200,000 cfs, max flow 50,000cfs
    # MP2023: project 243
    #     modeled at 50,000 cfs when the Mississippi River flow equals 1,000,000 cfs; 
    #     open with a variable flow rate calculated using a linear function from 0 to 50,000 cfs for river flow between 200,000 cfs and 1,000,000 cfs; 
    #     constant flow rate of 50,000 cfs for river flow above 1,000,000 cfs. No operation below 200,000 cfs
        
    impl_yr = implementation['AmaD']
    
    if yr <= impl_yr:
        Qdiv = 0
    else:
        if Qresidual < 200000:
            Qdiv = 0
        else:
            Qdiv = min(0.0625*Qresidual-12500, 50000)
        
    AmaD_cfs[d] = Qdiv  
    AmaD_cms[d] = Qdiv*(0.3048**3)
    Qresidual -= Qdiv
    
    
    ############################################
    ###   Inner Harbor Navigational Canal    ###
    ############################################
    # river mile 93
    # Diversion flow of rating curve 0.011297797*Qresidual
        
    impl_yr = implementation['IHNC']
    
    if yr <= impl_yr:
        Qdiv = 0
    else:
        if Qresidual >= 0:
            Qdiv = (Qresidual*0.3048**3)*0.011297797
        else:
            Qdiv = Qresidual
        
    IHNC_cfs[d] = Qdiv  
    IHNC_cms[d] = Qdiv*(0.3048**3)
    Qresidual -= Qdiv
     
    
    #######################################
    ###   Central Wetlands Diversion    ###
    #######################################
    # river mile 86
    # Diversion flow of rating curve 5,000 cfs at river flows above 5,000 cfs
    # MP2023: project 014a
    #     modeled at a constant flow of 5,000 cfs, independent of the Mississippi River flow
        
    impl_yr = implementation['CWDI']
    
    if yr <= impl_yr:
        Qdiv = 0
    else:
        if Qresidual >= 5000:
            Qdiv = 5000
        else:
            Qdiv = Qresidual
        
    CWDI_cfs[d] = Qdiv  
    CWDI_cms[d] = Qdiv*(0.3048**3)
    Qresidual -= Qdiv
    
    
    ############################################
    ###   Caernarvon Freshwater Diversion    ###
    ############################################
    # river mile 82
    # Diversion flow of rating curve 701.9143*ln(Qresidual)-5299.908567 with a maximum of 8828.66655 cfs
        
    impl_yr = implementation['Caer']
    
    if yr <= impl_yr:
        Qdiv = 0
    else:
        Qdiv = min(max(0,701.9143*np.log(Qresidual*0.3048**3)-5299.908567),8828.66655)
        
    Caer_cfs[d] = Qdiv  
    Caer_cms[d] = Qdiv*(0.3048**3)
    Qresidual -= Qdiv
    

    ###################################
    ###   Upper Breton Diversion    ###
    ###################################
    # river mile 77
    # 250,000 cfs version: Diversion flow following rating curve of 0.3125*Qresidual-62500 at river flows above 200,000 cfs
    # 75,000 cfs version: Diversion flow following rating curve of 0.3048*Qresidual-18750 at river flows above 200,000 cfs
    # MP2023:  project 013b
    #     modeled at 75,000 cfs when the Mississippi River flow equals 1,000,000 cfs; 
    #     open with a variable flow rate calculated using a linear function from 0 to 75,000 cfs for river flow between 200,000 cfs and 1,000,000 cfs; 
    #     constant flow rate of 75,000 cfs for river flow above 1,000,000 cfs. No operation below 200,000 cfs
        
    impl_yr = implementation['UBrD']

    if yr <= impl_yr:
        Qdiv = 0
    else:
        if Qresidual < 200000:
            Qdiv = 0
        else:
            Qdiv = 0.09375*Qresidual-18750      # 75,000 cfs Operations - opening threshold @ 250k
            #Qdiv = 0.3125*Qresidual-62500      # 250,000 cfs Operations - opening threshold @ 250k

    UBrD_cfs[d] = Qdiv  
    UBrD_cms[d] = Qdiv*(0.3048**3)
    Qresidual -= Qdiv

        
    
    ##########################################
    ###   Mid Breton Sediment Diversion    ###
    ##########################################
    # river mile 69
    # 75k diversion: Diversion flow of rating curve 0.06667*Qresidual-8333 with a minimum of 5,000 cfs
    # MP2023 FWOA using 55k diversion scenario: rating curve 0.04762*Qresidual-4524 with a minimum of 5,000 cfs, opens at 250,000 cfs
        
    impl_yr = implementation['MBrD']
    
    if yr <= impl_yr:
        Qdiv = 0
    else:
        #Qdiv = max(5000,0.04762*Qresidual-4524)     # 55,000 cfs Operations - opening threshold @ 250k
        #Qdiv = max(5000,0.06667*Qresidual-8333)     # 75,000 cfs Operations - opening threshold @ 250k
        Qdiv = max(5000,0.0625*Qresidual-23125)     # 55,000 cfs Operations - opening threshold @ 450k
        
    # alternative operations to maximize land building after year 20
    # 'turn off' diversion for two years after 20 years of sediment deposition to allow for vegetation establishment
    if yr in [2041,2042]:
        Qdiv = 5000
    # alternate ops with Mid Barataria - even years Mid-Breton will flow, odd years, Mid-Barataria will flow
    if yr > 2042:
        if yr in range(2044,2072,2):
            Qdiv = 5000
        
    MBrD_cfs[d] = Qdiv
    MBrD_cms[d] = Qdiv*(0.3048**3)
    Qresidual -= Qdiv
    

    ###################################
    ###   Naomi Siphon Diversion    ###
    ###################################
    # river mile 64
    # Diversion flow of rating curve 281.044708*ln(Qresidual)-2500.93169 with a maximum of 2118.87997 cfs

    impl_yr = implementation['Naom']
    
    if yr <= impl_yr:
        Qdiv = 0
    else:
        Qdiv = min(max(0,281.044708*np.log(Qresidual*0.3048**3)-2500.93169),2118.87997)
        
    Naom_cfs[d] = Qdiv  
    Naom_cms[d] = Qdiv*(0.3048**3)
    Qresidual -= Qdiv

    
    #############################################
    ###   Mid-Barataria Sediment Diversion    ###
    #############################################
    # river mile 61
    # 5 different versions:
        # 75k @ 1.0 m : Diversion flow of rating curve 0.09375*residual - 18750 at river flows above 200,000
        # 250k @ 1.0 m : Diversion flow of rating curve 0.3125*residual - 62500 at river flows above 200,000
        # 35k - 75k @ 1.0 m : Diversion flow of rating curve 0.04375*residual - 8750 at river flows above 200,000
        # 75k @ 1.25 m , 5k min : Diversion flow of rating curve 0.06667*residual - 8333 with a minimum of 5,000 cfs, opens at 250,000 cfs
        # 75k @ 1.25 m , 5k min : Diversion flow of rating curve 0.08757*residual - 34375 with a minimum of 5,000 cfs, opens at 450,000 cfs
    impl_yr = implementation['MBaD']

    if yr <= impl_yr:
        Qdiv = 0
    else:
#        if Qresidual < 200000:
#            Qdiv = 0
#        else::
#            Qdiv = 0.09375*Qresidual - 18750       # 75k @ 1.0 m 
#            Qdiv = 0.3125*Qresidual-62500          # 250k @ 1.0 m 
#            Qdiv = 0.04375*Qresidual-8750          # 35k - 75k @ 1.0 m 
#        Qdiv = max(5000, 0.06667*Qresidual-8333)   # 75k @ 1.25 m , 5k min, opening threshold @ 250k
        Qdiv = max(5000, 0.0875*Qresidual-34375)    # 75k @ 1.25 m , 5k min, opening threshold @ 450k
    
    # alternative operations to maximize land building after year 20
    # 'turn off' diversion for two years after 20 years of sediment deposition to allow for vegetation establishment
    if yr in [2041,2042]:
        Qdiv = 5000
    # alternate ops with Mid Barataria - even years Mid-Breton will flow, odd years, Mid-Barataria will flow
    if yr > 2042:
        if yr in range(2043,2071,2):
            Qdiv = 5000
  
    MBaD_cfs[d] = Qdiv
    MBaD_cms[d] = Qdiv*(0.3048**3)
    Qresidual -= Qdiv
    

    ###################################
    ###   West Pointe a la Hache    ###
    ###################################
    # river mile 49
    # Diversion flow of rating curve 456.35377*ln(Qresidual)-4049.4586 with a maximum of 2118.87997 cfs
        
    impl_yr = implementation['WPLH']
    
    if yr <= impl_yr:
        Qdiv = 0
    else:
        Qdiv = min(max(0, 456.35377*np.log(Qresidual*0.3048**3)-4049.4586),2118.87997)
        
    WPLH_cfs[d] = Qdiv 
    WPLH_cms[d] = Qdiv*(0.3048**3)
    Qresidual -= Qdiv
    
    
    ################################################
    ###   Lower Plaquemines River Sediment Plan  ###
    ################################################
    # river mile 49 
    # MP2023: project 327
    # Seven pumps/siphons located throughout the Mississippi River corridor
    # (assume all pumps/siphons are extracted at one location in the river located at West Point a la Hache)
    #
    # Each siphon is operated with the same rating curve ( Qresidual/225 - 1333.3 ):
    #     No flow diverted when river < 300,000 cfs
    #     Maximum flow diverted of 2,000 cfs when river > 750,000 cfs
    #     Linear relationship when river between 300,000 and 750,000 cfs
    #     Operated December 1 through April 30
    # 
    # Since all 7 pump/siphons are being extracted from the Mississippi River flow timeseries at one location (assumed to be at WPLH)
    # this run will also assume that WPLH is operated with this same new operational curve
    # therefore, must add calculated WPLH back into Qresidual before being added and this diversion timeseries overwrites the WPLH timeseries in TribQ.csv
    
    impl_yr = implementation['LPlq']
    
    if yr <= impl_yr:
        Qdiv = 0
    else:
        if month > 4 and month < 12:
            Qdiv = 0
        else:
            # diversion flowrate is 8x original WPLH flows...WPLH is added above, add 7 additional if this diversion is active
            Qdiv = 7.0*min(max(0, 456.35377*np.log(Qresidual*0.3048**3)-4049.4586),2118.87997)

        LPlq_cfs[d] = Qdiv
        LPlq_cms[d] = Qdiv*(0.3048**3)
 
        Qresidual  -= Qdiv    
    
    
    ######################################
    ###   Lower Barataria Diversion    ###
    ######################################
    # river mile 40
    # Diversion flow at rating curve of 0.0625*residual-12500 at river flows above 200,000 cfs
        
    impl_yr = implementation['LBaD']
    
    if yr <= impl_yr:
        Qdiv = 0
    else:
        if Qresidual < 200000:
            Qdiv = 0
        else:
            Qdiv = 0.0625*Qresidual-12500
        
    LBaD_cfs[d] = Qdiv 
    LBaD_cms[d] = Qdiv*(0.3048**3)
    Qresidual -= Qdiv
        
        
    ###################################
    ###   Lower Breton Diversion    ###
    ###################################
    # river mile 37
    # Diversion flow of rating curve 0.0625*residual-12500 at river flows above 200,000 cfs
    # MP2023: project 006
    #    modeled at 50,000 cfs when the Mississippi River flow equals 1,000,000 cfs; 
    #    open with a variable flow rate calculated using a linear function from 0 to 50,000 cfs for river flow between 200,000 cfs and 1,000,000 cfs; 
    #    constant flow rate of 50,000 cfs for river flow above 1,000,000 cfs. No operation below 200,000 cfs	
        
    impl_yr = implementation['LBrD']
    
    if yr <= impl_yr:
        Qdiv = 0
    else:
        if Qresidual < 200000:
            Qdiv = 0
        else:
            Qdiv = min(0.0625*Qresidual-12500, 50000)
        
    LBrD_cfs[d] = Qdiv  
    LBrD_cms[d] = Qdiv*(0.3048**3)
    Qresidual -= Qdiv

    ######################################################################
    ######      Calculate distributary flow at each BFD pass        ######
    ######      Qresidual is no longer updated downstream of here   ######
    ######      All passes will use the same input residual flow    ######
    ######      rating curves come from Mead Allison rating curves  ######
    ######################################################################
        
    ############################
    ###   Mardi Gras Pass    ###
    ############################
    # river mile 44
    # Diversion flow of rating curve 0.0153*Qresidual+276.2369
        
    Qdiv = 0.0153*Qresidual+276.2369
        
    MGPS_cfs[d] = Qdiv
    MGPS_cms[d] = Qdiv*(0.3048**3)
        
    #############################
    ###   Bohemia Spillway    ###
    #############################
    # river mile 44 to 34
    # Diversion flow of rating curve 1.4/100*Qresidual at river flows above 930,000 cfs
        
    if Qresidual < 930000:
        Qdiv = 0
    else:
        Qdiv = 1.4/100*Qresidual
    
    Bohe_cfs[d] = Qdiv  
    Bohe_cms[d] = Qdiv*(0.3048**3)
        
    #################################
    ###   Ostrica & Neptune Pass  ###
    #################################
    # river mile 25
    # if input is less than 800,000 cfs, then 0 is diverted through original Ostrica
    # Ostrica diversion flow of rating curve 5.2/100*Qresidual at river flows above 800,000 cfs
    #
    # at Neptune Pass, flow is assumed to be a constant 5% of the Mississippi River flow which represents the flows after USACE paritally closed the pass in spring 2026
    # if modeling from 2019 through end of 2025, flow reached a maximum of ~19% of the River flow before USACE partially closed the opening in spring 2026
    # 5% & 19% rules-of-thumb value comes from Mead Allison during MissDelta Field Team project calls February 2026
    
    if Qresidual < 800000:
        Qdiv_Ostr = 0
    else:
        Qdiv_Ostr = 5.2/100*Qresidual
    
    Qdiv_Neptune = 0.05*Qresidual
    
    Ostr_cfs[d] = Qdiv_Ostr + Qdiv_Neptune
    Ostr_cms[d] = (Qdiv_Ostr + Qdiv_Neptune)*(0.3048**3)

    ############################
    ###   Fort St. Philip    ###
    ############################
    # river mile 20
    # Diversion flow of rating curve 0.1011*Qresidual-25159
        
    Qdiv = max(0.0,0.1011*Qresidual-25159)
        
    FStP_cfs[d] = Qdiv  
    FStP_cms[d] = Qdiv*(0.3048**3)

    ##############################
    ###   Baptiste Collette    ###
    ##############################
    # river mile 11
    # Diversion flow of rating curve 0.1031*Qresidual-5631
        
    Qdiv = 0.1031*Qresidual-5631
        
    Bapt_cfs[d] = Qdiv  
    Bapt_cms[d] = Qdiv*(0.3048**3)

    #######################
    ###   Grand Pass    ###
    #######################
    # river mile 11
    # Diversion flow of rating curve 0.0915*Qresidual+4288
        
    Qdiv = 0.0915*Qresidual+4288
        
    GrPa_cfs[d] = Qdiv  
    GrPa_cms[d] = Qdiv*(0.3048**3)

    #####################
    ###   West Bay    ###
    #####################
    # river mile 5
    # Diversion flow of rating curve 0.0653*Qresidual-2075
        
    Qdiv = 0.0653*Qresidual-2075
        
    WBay_cfs[d] = Qdiv  
    WBay_cms[d] = Qdiv*(0.3048**3)

    #######################
    ###   Small Cuts    ###
    #######################
    # This is the amount diverted through all of the small outlets throughout the delta that aren't accounted for by the outlets here
    # the total amount diverted through all of these is 0.0025*Qresidual+10196
        
    Qdiv = 0.0025*Qresidual+10196
        
    SCut_cfs[d] = Qdiv  
    SCut_cms[d] = Qdiv*(0.3048**3)

    ########################
    ###   Cubit's Gap    ###
    ########################
    # river mile 3
    # Diversion flow of rating curve 0.1319*Qresidual-19939
        
    Qdiv = 0.1319*Qresidual-19939
        
    CGap_cfs[d] = Qdiv  
    CGap_cms[d] = Qdiv*(0.3048**3)

    #######################
    ###   South Pass    ###
    #######################
    # river mile 0
    # Diversion flow of rating curve 0.0858*Qresidual+2332
        
    Qdiv = 0.0858*Qresidual+2332
        
    SPas_cfs[d] = Qdiv 
    SPas_cms[d] = Qdiv*(0.3048**3)
 
    ##########################
    ###   Pass a Loutre    ###
    ##########################
    # river mile 0
    # Diversion flow of rating curve 0.0543*Qresidual+15700
        
    Qdiv = 0.0543*Qresidual+15700
        
    PLou_cfs[d] = Qdiv  
    PLou_cms[d] = Qdiv*(0.3048**3)
 
    #########################################
    ###   Southwest Pass Ratings Curve    ###
    #########################################
    # river mile 0
    # this is the ratings curve for SW pass, there is also a SW Pass Residual calculation below
    # Diversion flow of rating curve 0.4189*Qresidual-64787
        
    Qdiv = 0.4189*Qresidual-64787
        
    SWPS_cfs[d] = Qdiv 
    SWPS_cms[d] = Qdiv*(0.3048**3)
    
    ####################################
    ###   Southwest Pass Residual    ###
    ####################################
    # river mile 0
    # the amount diverted here is calculated by finding the residual output after all other distributaries are diverted
        
    Qdiv = Qresidual - (MGPS_cfs[d] + Bohe_cfs[d] + Ostr_cfs[d] + FStP_cfs[d] + Bapt_cfs[d] + GrPa_cfs[d] + WBay_cfs[d] + SCut_cfs[d] + CGap_cfs[d] + SPas_cfs[d] + PLou_cfs[d])
        
    SWPR_cfs[d] = Qdiv
    SWPR_cms[d] = Qdiv*(0.3048**3)


####################################
###   write new TribQ.csv file   ###
####################################

if process_observed == True:
    TribQ_to_write = TribQ_observed_out_file
else:
    TribQ_to_write = TribQ_future_out_file

with open(TribQ_to_write,mode='w') as TribQ_out:
    # write header line to TribQ.csv
    line = '1'
    for n in range(2,nTribs):
        line = '%s,%s' % (line,n)
    
    TribQ_out.write('%s,! yyyy-mm-dd\n' % line)          
    for d in range(0,ndays):
        # write tributary flow read in from original TribQ.csv
        line = '%s' % TribQ_in[d][0]                    # ncol 01 # Neches River at Beaumont TX
        for t in range(1,nTributaries+nTributaries_null+nTributaries_calc):
            line = '%s,%s' % (line,TribQ_in[d][t])      # ncol 02 # Sabine River at Ruliff TX
                                                        # ncol 03 # Vinton Canal
                                                        # ncol 04 # Calcasieu River near Kinder LA
                                                        # ncol 05 # Bayou Lacassine near Lake Arthur LA
                                                        # ncol 06 # Mermentau River at Mermentau LA
                                                        # ncol 07 # Vermilion River at Surrey St at Lafayette LA
                                                        # ncol 08 # Charenton Drainage Canal at Baldwin LA
                                                        # ncol 09 # GIWW at Franklin
                                                        # ncol 10 # Atch_cms #Atchafalaya River
                                                        # ncol 11 # Mississippi River Upstream (Tarbert Landing)
                                                        # ncol 12 # GIWW at Larose
                                                        # ncol 13 # Bayou Lafourche at Thibodeaux LA
                                                        # ncol 14 # Amite River near Denham Springs LA
                                                        # ncol 15 # Natalbany River at Baptist LA
                                                        # ncol 16 # Tickfaw River at Holden LA
                                                        # ncol 17 # Tangipahoa River at Robert LA
                                                        # ncol 18 # Tchefuncte River near Folsom LA
                                                        # ncol 19 # Bogue Chitto near Bush LA
                                                        # ncol 20 # Pearl River near Bogalusa LA
                                                        # ncol 21 # Wolf River near Landon MS
                                                        # ncol 22 # Biloxi River at Wortham MS
                                                        # ncol 23 # Pascagoula River at Merrill MS
                                                        # ncol 24 # Tensaw River near Mount Vernon AL
                                                        # ncol 25 # Mobile River at River Mile 31 at Bucks AL
                                                        # ncol 26 # Mobile1
                                                        # ncol 27 # Mobile 2
                                                        # ncol 28 # Jourdan
                                                        # ncol 29 # Violet Runoff
                                                        # ncol 30 # NE Lake Pontchartrain ungaged drainage (Bayou Bonfouca)
                                                        # ncol 31 # SE Lake Pontchartrain ungaged drainage (Orleans Parish)
                                                        # ncol 32 # SW Lake Pontchartrain ungaged drainage (Jefferson Parish)
                                                        # ncol 33 # SW Lake Pontchartrain ungaged drainage
                                                        # ncol 34 # S Lake Maurepas ungaged drainage
                                                        # ncol 35 # NE Lake Pontchartrain ungaged drainage (Bayou LaCombe)      

        # write calculated diversion/pass flow calculated above
        line = '%s,%s' % (line,Morg_cms[d])             # ncol 36 # Morganza Spillway
        line = '%s,%s' % (line,BLaF_cms[d])             # ncol 37 # Bayou LaFourche Diversion
        line = '%s,%s' % (line,FDWB_cms[d])             # ncol 38 # Freshwater Delivery to Western Barataria
        line = '%s,%s' % (line,UBaH_cms[d])             # ncol 39 # Upper Barataria Hydrologic Restoration
        line = '%s,%s' % (line,UFWD_cms[d])             # ncol 40 # Union Freshwater Diversion
        line = '%s,%s' % (line,WMPD_cms[d])             # ncol 41 # West Maurepas Diversion
        line = '%s,%s' % (line,MSRM_cms[d])             # ncol 42 # Mississippi River Reintroduction in Maurepas Swamp (East Maurepas Diversion in 2017 MP)
        line = '%s,%s' % (line,EdDI_cms[d])             # ncol 43 # Edgard Diversion
        line = '%s,%s' % (line,Bonn_cms[d])             # ncol 44 # Bonnet Carre
        line = '%s,%s' % (line,MLBD_cms[d])             # ncol 45 # Manchac Landbridge Diversion
        line = '%s,%s' % (line,LaBr_cms[d])             # ncol 46 # LaBranche Hydrologic Restoration
        line = '%s,%s' % (line,DavP_cms[d])             # ncol 47 # Davis Pond
        line = '%s,%s' % (line,AmaD_cms[d])             # ncol 48 # Ama Sediment Diversion
        line = '%s,%s' % (line,IHNC_cms[d])             # ncol 49 # Inner Harbor Navigational Canal
        line = '%s,%s' % (line,CWDI_cms[d])             # ncol 50 # Central Wetlands Diversion
        line = '%s,%s' % (line,Caer_cms[d])             # ncol 51 # Caernarvon
        line = '%s,%s' % (line,UBrD_cms[d])             # ncol 52 # Upper Breton Diversion
        line = '%s,%s' % (line,MBrD_cms[d])             # ncol 53 # Mid-Breton Sound Diversion
        line = '%s,%s' % (line,MBaD_cms[d])             # ncol 54 # Mid-Barataria Diversion
        line = '%s,%s' % (line,Naom_cms[d])             # ncol 55 # Naomi
        line = '%s,%s' % (line,WPLH_cms[d])             # ncol 56 # West Point a la Hache
        line = '%s,%s' % (line,LPlq_cms[d])             # ncol 57 # Lower Plaquemines River Sediment Plan)
        line = '%s,%s' % (line,LBaD_cms[d])             # ncol 58 # Lower Barataria Diversion
        line = '%s,%s' % (line,LBrD_cms[d])             # ncol 59 # Lower Breton Diversion
        line = '%s,%s' % (line,MGPS_cms[d])             # ncol 60 # Mardi Gras Pass
        line = '%s,%s' % (line,Bohe_cms[d])             # ncol 61 # Bohemia
        line = '%s,%s' % (line,Ostr_cms[d])             # ncol 62 # Ostrica
        line = '%s,%s' % (line,FStP_cms[d])             # ncol 63 # Ft St Phillip
        line = '%s,%s' % (line,Bapt_cms[d])             # ncol 64 # Baptiste Collette
        line = '%s,%s' % (line,GrPa_cms[d])             # ncol 65 # Grand Pass
        line = '%s,%s' % (line,WBay_cms[d])             # ncol 66 # West Bay Diversion
        line = '%s,%s' % (line,SCut_cms[d])             # ncol 67 # SmallCuts
        line = '%s,%s' % (line,CGap_cms[d])             # ncol 68 # Cubits Gap
        line = '%s,%s' % (line,PLou_cms[d])             # ncol 69 # Pass A Loutre
        line = '%s,%s' % (line,SPas_cms[d])             # ncol 70 # South Pass
        #line = '%s,%s' % (line,SWPS_cms[d])             # ncol 71 # South West Pass calculated from curve (not used in model)
        line = '%s,%s' % (line,SWPR_cms[d])             # ncol 71 # South West Pass calculated from residual flow to close mass balance on Miss Riv flow in/out
        line = '%s,%s' % (line,IAFT_cms[d])             # ncol 72 # Increase Atchafalaya Flows to Terrebonne
        line = '%s,%s' % (line,AtRD_cms[d])             # ncol 73 # Atchafalaya River Diversion
        #line = '%s,%s' % (line,LaBD_cms[d])                       # LaBranche Diversion
        line = '%s,! %s' % (line, dates_all[d])         # ncol 74 # Date
        
        TribQ_out.write('%s\n' % line)


if process_sediment == True:
    if process_observed == True:
        TribF_to_write = TribF_observed_out_file
        TribS_to_write = TribS_observed_out_file
    else:
        TribF_to_write = TribF_future_out_file
        TribS_to_write = TribF_future_out_file

    #####################################################################
    ###   calculate suspended sediment concentrations from flow       ###
    ###   rating curves and write new TribF.csv and TribS.csv files   ###
    #####################################################################
    print('reading in formatted TribQ.csv to calculate suspsended sands and fines concentrations.')
    flows_cms = np.genfromtxt(TribQ_to_write,delimiter=',',dtype='float',skip_header=1,usecols=range(0,nTribs))
    dates = np.genfromtxt(TribQ_to_write,delimiter=',',dtype='str',skip_header=1,usecols=[-1]) 
    
    with open(TribF_to_write,mode='w') as fineout:
        with open(TribS_to_write,mode='w') as sandout:
            # write header line
            line = '1'
            for n in range(2,nTribs+1):
                line = '%s,%s' % (line,n)
            fineout.write('%s\n' % line)
            sandout.write('%s\n' % line)
                
            for nday in range(0,len(dates)):
                dateout = dates[nday]       # dateout will have the '!  ' prepended to the date string from being read in from TribQ.csv
                row = flows_cms[nday]
                
                fineline = ''
                sandline = ''
    
                for ntrib in range(0,len(row)):
                    sand_mgl = 0.0          # initialize to 0 mg/L
                    fine_mgl = 0.0          # initialize to 0 mg/L
                    q_cms = row[ntrib]
            
                    # set local copies of tributary-specific variables that were read in from input file
                    tribcol = tribs_col[ntrib]
                    sand_type = sand_types[tribcol]                     # integer storing sand rating curve type id
                    fine_type = fine_types[tribcol]                    # integer storing fines rating curve type id
                    trib_area = TSS_trib_areas[tribcol]                 # float storing the tributary area upstream of gage used for TSS rating curves for Florida Parishes tributaries with limited TSS data (see MP23 Appendix B2, section 5.5)
                    q_maxsand = TSS_qmaxsands[tribcol]                  # float storing flowrate (cms) used to define the maximum flow where peak sand suspension occurs - used to partition TSS into sands and fines (see MP23 Appendix B2, section 5.5)
                    max_sand_portion = TSS_max_sand_portions[tribcol]   # float storing maximum portion of TSS that can is sand (derived from Miss. River data) - used to partition TSS into sands and fines (see MP23 Appendix B2, section 5.5)
                        
                    tpd2mgl = 1000*1000*1000/(max(0.01,q_cms)*1000*86400)         # flow-specific conversion factor for tonnes/day to mg/L    (load to concentration) - max function is to prevent div/zero errors
                    kgps2mgl = 1000*1000/(max(0.01,q_cms)*1000)         # flow-specific conversion factor for kg/sec to mg/L        (load to concentration) - max function is to prevent div/zero errors
            
            
                    ###################################################################
                    # Rating curves for gages without any suspended sediment boundary #
                    ###################################################################
                    # gage is either not used as an ICM boundary conditon or is only used as a freshwater boundary - no sediment timeseries applied at this boundary in the ICM
            
                    # Assume no suspended sand
                    if sand_type == 0:
                        sand_mgl = 0.0
            
                    # Assume no suspended fines
                    if fine_type == 0:
                        fine_mgl = 0.0
            
                    ###########################################################################
                    # Rating curves for gages with Suspended Sand Sediment concentration data #
                    ###########################################################################
                    # Assume no suspended sand - gage is either not used as an ICM boundary conditon or is only used as a freshwater boundary - no sediment timeseries applied at this boundary in the ICM
                    if sand_type == 0:
                        sand_mgl = 0.0
            
                    # Mississippi River sand rating curve - original curve is in sediment load (tonnes/day)
                    if sand_type == 1:
                        sand_tpd = 77160000*(1.0 - np.e**(-0.0000002485*q_cms)) - 574800*(1.0 - np.e**(-0.00004122*q_cms))
                        sand_mgl = sand_tpd*tpd2mgl        
            
                    # Atchafalaya River sand rating curve - original curve is in sediment load (tonnes/day)
                    if sand_type == 2:
                        sand_mgl = 0.0001113*(q_cms**1.4897)
            
                    ###########################################################################
                    # Rating curves for gages with Suspended Fine Sediment concentration data #
                    ###########################################################################
            
                    # Mississippi River fines rating curve - original curve is in sediment load (tonnes/day)
                    if fine_type == 1:
                        fine_tpd =0.002*(q_cms**1.86)
                        fine_mgl = fine_tpd*tpd2mgl 
            
                    # Atchafalaya River fines rating curve - original curve is in sediment load (tonnes/day)
                    if fine_type == 2:
                        fine_mgl = 4948.5*(q_cms**-0.356)
            
            
                    #################################################################################
                    # Rating curves for gages with only Total Suspended Sediment Concentration data #
                    #################################################################################
                    # TSS rating curves for all tributaries that do not have enough data for separate rating curves for sands and fines
                    #   after calculating TSS in mg/L - the TSS will be partitioned into portions sands and fines as a function of discharge - see MP23 Appendix B2, section 5.5 for documentation
                    if sand_type in [3,4,5,6,7]:
                    # Tributaries west of Mississippi River (excluding Atchafalaya) - total suspended sediment load rating curve from all USGS paired Q-TSS data west of Miss. River - partitioned into sand/fines
                        if sand_type == 3:
                            tss_kg_sec = 0.0382*(q_cms**1.099)
                            tss_mgl = tss_kg_sec*kgps2mgl
            
                        # Florida Parishes TSS rating curve based upon upstream drainage area (from Rachel Roblin MS thesis 2008, UNO)
                        if sand_type == 4:
                            tss_mgl = 95.8189*((q_cms/trib_area)**0.2678)
                    
                        # Tangipahoa River TSS rating curve (from USGS paired TSS-Q data)
                        if sand_type == 5:
                            tss_mgl = 3.231*(q_cms**0.7867)
            
                        # Bouge Chitto TSS rating curve (from USGS paired TSS-Q data)
                        if sand_type == 6:
                            tss_mgl = 6.3791*(q_cms**0.4833)
            
                        # Pearl River TSS rating curve (from USGS paired TSS-Q data) 
                        if sand_type == 7:
                            tss_mgl = 3.0127*(q_cms**0.5987)		
            
                        # partition TSS concentration into portion that is sand (based on MP23 analysis of sand/fines ratio in the Mississippi River - see MP23 Appendix B2, section 5.5)
                        q_qmx = q_cms/q_maxsand
                        sand_portion = max_sand_portion*(30.292*q_qmx**5 - 113.25*q_qmx**4 + 169.9*q_qmx**3 - 129.38*q_qmx**2 + 51.167*q_qmx- 7.7249)
                        sand_portion_capped = min(max(0,sand_portion),max_sand_portion)    # apply high-pass filter to avoid negative portion sands and apply low-pass filter to cap portion sand at default value (max_sand_portion) read in from input file
            
                        sand_mgl = tss_mgl*sand_portion_capped
                        fine_mgl = tss_mgl - sand_mgl
            
                    # high-pass filter to prevent negative concentrations    
                    sand_mgl = max(0,sand_mgl)
                    fine_mgl = max(0,fine_mgl)
                    
                    # append concentrations to daily line that will be written to file
                    if ntrib == 0:
                        fineline = '%0.4f' % (fine_mgl)
                        sandline = '%0.4f' % (sand_mgl)
                    else:
                        fineline = '%s,%0.4f' % (fineline,fine_mgl)
                        sandline = '%s,%0.4f' % (sandline,sand_mgl)
                    
                    
                    
                fineout.write('%s,%s\n' % (fineline,dateout))
                sandout.write('%s,%s\n' % (sandline,dateout))
    