import requests
import csv
import numpy as np
import datetime as dt
import calendar
import scipy
from scipy import stats
import matplotlib.pyplot as plt

download_files = False

#years = range(2006,2020)
#noaa_gages = [8770570]

#years = range(2012,2024)
#noaa_gages = [8770822]

years = range(2006,2024)
noaa_gages = [8735180,8760922,8761724,8764227,8768094,8771450]

#years = range(2006,2024)
#noaa_gages = [73813498]

gage_survey = {}    # gage benchmark data from site datum and benchmark sheets
gage_survey[8735180] = {'date':'2021-02-01','NAVD88_rel_MTL_m':-0.026}
gage_survey[8760922] = {'date':'2018-02-27','NAVD88_rel_MTL_m':-0.313} # NAVD88 conversion from VDatum, as determined for MP2023 - survey date from Datum Sheet, not benchmark sheet (VDatum conversion on 12/04/2024 is essentially unchanged = -0.310)
gage_survey[8761724] = {'date':'2018-02-16','NAVD88_rel_MTL_m':-0.053} # NAVD88 conversion from VDatum on 12/05/2024 - survey date from benchmark sheet
gage_survey[8764227] = {'date':'2020-11-17','NAVD88_rel_MTL_m': 0.008}
gage_survey[8768094] = {'date':'2020-12-31','NAVD88_rel_MTL_m': 0.094}
gage_survey[8770570] = {'date':'2011-09-27','NAVD88_rel_MTL_m':-0.065} # NAVD88 conversion from matching eustatic-adjusted levels from 2012-2020 against other Sabine Pass gage (8770822)
gage_survey[8770822] = {'date':'2020-05-01','NAVD88_rel_MTL_m':-0.010}
gage_survey[8771450] = {'date':'2019-12-09','NAVD88_rel_MTL_m':-0.154}
gage_survey[73813498] = {'date':'2017-05-21','NAVD88_rel_MTL_m':0.125} # per archive.org the gage datum conversion was published by at least 5/21/2017 (https://web.archive.org/web/20170521154638/https://waterdata.usgs.gov/nwis/inventory/?site_no=073813498&agency_cd=USGS);  it was not published on the previously available archive snapshot taken on 9/25/2015 (https://web.archive.org/web/20150925133535/http://waterdata.usgs.gov/nwis/inventory/?site_no=073813498&agency_cd=USGS)

gage_names = {}
gage_names[8735180] = 'Dauphin Island AL'
gage_names[8760922] = 'Pilots Station East SW Pass LA'
gage_names[8761724] = 'Grand Isle LA'
gage_names[8764227] = 'LAWMA Amerada Pass LA'
gage_names[8768094] = 'Calcasieu Pass LA'
gage_names[8770570] = 'Sabine Pass North TX'
gage_names[8770822] = 'Texas Point Sabine Pass TX'
gage_names[8771450] = 'Galveston Pier 21 TX'
gage_names[73813498] = 'Caillou Bay SW of Cocodrie LA'

gage_loc = {}
gage_loc[8735180] = '30° 15.0 N, 88° 4.5 W'
gage_loc[8760922] = '28° 55.9 N, 89° 24.4 W'
gage_loc[8761724] = '29° 15.8 N, 89° 57.4 W'
gage_loc[8764227] = '29° 27.0 N, 91° 20.3 W'
gage_loc[8768094] = '29° 46.1 N, 93° 20.6 W'
gage_loc[8770570] = '29° 43.7 N, 93° 52.2 W'
gage_loc[8770822] = '29° 41.4 N, 93° 50.5 W'
gage_loc[8771450] = '29° 18.6 N, 94° 47.6 W'
gage_loc[73813498] = '29° 4.7 N, 90° 52.3 W'


# subsidence rates (mm/yr) extracted from 2025 CPRA subsidence map (Vincent, 2025)
gage_sub_cpra_mm_yr = {}
gage_sub_cpra_mm_yr[8735180] = 4.33
gage_sub_cpra_mm_yr[8760922] = 31.93
gage_sub_cpra_mm_yr[8761724] = 5.95
gage_sub_cpra_mm_yr[8764227] = 6.60
gage_sub_cpra_mm_yr[8768094] = 3.19
gage_sub_cpra_mm_yr[8770570] = 3.75
gage_sub_cpra_mm_yr[8770822] = 4.83
gage_sub_cpra_mm_yr[8771450] = -9999 # outside of CPRA subsidence map extent
gage_sub_cpra_mm_yr[73813498] = 6.91


datum = 'MTL'
tz = 'GMT'
units = 'english'
conv = 0.3048   # conversion factor for feet-to-meters
GOM_ESLR_mm_yr = 5.1 # pulled 2024-11-20 from https://www.star.nesdis.noaa.gov/socd/lsa/SeaLevelRise/LSA_SLR_timeseries_regional.php (MP2023 value = 3.6)

GOM_ESLR_mm_hr = GOM_ESLR_mm_yr/(365.25*24)
starttime = dt.datetime(years[0],1,1,0,0)
endtime = dt.datetime(years[-1]+1,1,1,0,0)
all_hours = range(0,int((endtime - starttime).total_seconds()/(60*60)))
print(' - calculating subsidence and correcting RSLR to ESLR.')

for site in noaa_gages:
    print('\nprocessing %s: %s' % (site,gage_names[site]))
    
    surv_y, surv_m, surv_d = gage_survey[site]['date'].split('-')
    surveytime = dt.datetime(int(surv_y),int(surv_m),int(surv_d),12,0)

    obs_file = 'observed/NOAA_%d_%d-%d_observed_20241008.csv' % (site,years[0],years[-1])
    prd_file = 'predicted/NOAA_%d_%d-%d_predicted_20241008.csv' % (site,years[0],years[-1])

    # if site is the USGS Caillou Bay gage, use pre-downloaded hourly data and fill missing data with predicted tidal signal from LAWMA Amerada Pass NOAA gage
    if site == 73813498:
        obs_file = 'observed/USGS_%d_%d-%d_observed_20241204.csv' % (site,years[0],years[-1])
        prd_file = 'predicted/NOAA_8764227_%d-%d_predicted_20241008.csv' % (years[0],years[-1])
        
    if download_files == True:
        with open('%s.log' % obs_file,mode='w') as outlog:
            with open(obs_file,mode='wb') as out:
                for year in years:
                    print('             - downloading observed data: %s' % year)
                    obs_url = 'https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?product=hourly_height&application=NOS.COOPS.TAC.WL&begin_date=%04d0101&end_date=%04d1231&datum=%s&station=%d&time_zone=%s&units=%s&format=csv' % (year,year,datum,site,tz,units)
                    outlog.write('observed API URL: %s\n' % obs_url)
                    with requests.Session() as s:
                        download = s.get(obs_url)
                        decoded_content = download.content.decode('utf-8')
                        cr = csv.reader(decoded_content.splitlines(), delimiter=',')
                        ww = csv.writer(out,delimiter=',')
                        if year == years[0]:
                            for row in cr:
                                ww.writerow(row)
                        else:        
                            for row in cr:
                                if row[0] != 'D':
                                    ww.writerow(row)

            with open(prd_file,mode='wb') as out:
                for year in years:
                    print(' - downloading predicted tides: %s' % year)
                    prd_url = 'https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?product=predictions&application=NOS.COOPS.TAC.WL&begin_date=%04d0101&end_date=%04d1231&datum=%s&station=%d&time_zone=%s&units=%s&interval=h&format=csv' % (year,year,datum,site,tz,units)
                    outlog.write('predicted API URL: %s\n' % prd_url)
                    with requests.Session() as s:
                        download = s.get(prd_url)
                        decoded_content = download.content.decode('utf-8')
                        cr = csv.reader(decoded_content.splitlines(), delimiter=',')
                        ww = csv.writer(out,delimiter=',')
                        if year == years[0]:
                            for row in cr:
                                ww.writerow(row)
                        else:        
                            for row in cr:
                                if row[0] != 'D':
                                    ww.writerow(row)

            

    observed = np.genfromtxt(obs_file,delimiter=',',usecols=[0,1],dtype=str)
    predicted = np.genfromtxt(prd_file,delimiter=',',usecols=[0,1],dtype=str)
    
    if len(observed) != len(predicted):
        print('!!!!ERROR!!!!\nMismatch in length of downloaded observed and predicted records. Aborting.')
        #break
    
    header = observed[0]
    dates = []
    hours = []
    hours_since_survey = []
    relative_levels = []
    flags = []

    missing_count = 0
    
    for nrow in range(0,len(observed)):
        record = observed[nrow]
        if record[0] != header[0]:
            ds,ts = record[0].split()
            y,mo,d = ds.split('-')
            h,mi = ts.split(':')
            timestamp = dt.datetime(int(y),int(mo),int(d),int(h),int(mi))

            dates.append(timestamp)
            hours.append(int((timestamp - starttime).total_seconds()/(60*60)))
            hours_since_survey.append(int((timestamp - surveytime).total_seconds()/(60*60)))

            # read in observed data record and convert units, TRY will fail if missing data so EXCEPT will fill with predicted data
            try:  
                val = float(record[1])*conv
                relative_levels.append(val)
                missing_count = 0
                flags.append(0)
            except:
                #print('%s no observed data record' % record[0])
                pred = float(predicted[nrow][1])*conv

                # if first missing value determine offset between first predicted value and extrapolated value from last two obervations
                # if missing from start of dataset, TRY will fail and offset will default to 0
                if missing_count == 0:
                    try:
                        obs_rise = relative_levels[-1] - relative_levels[-2]
                        expected = relative_levels[-1] + obs_rise
                        offset = relative_levels[-1] - pred
                    except:
                        offset = 0.0

                relative_levels.append(pred+offset)
                missing_count += 1
                flags.append(1)

        nrow += 1
    
    if len(hours) != len(hours_since_survey):
        print('!!!!ERROR!!!!\nMismatch in length of timestep and value records. Aborting.')
        break
    elif len(hours) != len(relative_levels):
        print('!!!!ERROR!!!!\nMismatch in length of timestep and value records. Aborting.')
        break

        
    RSLR_hr_regress = scipy.stats.linregress(hours,relative_levels)
    RSLR_m_hr = RSLR_hr_regress[0]
    RSLR_mm_hr = RSLR_m_hr*1000
    sub_mm_hr = RSLR_mm_hr - GOM_ESLR_mm_hr

    eustatic_levels = []

    for i in range(0,len(hours)):
        sub_mm_cumul = sub_mm_hr*hours_since_survey[i]

        corrected_level = relative_levels[i] - sub_mm_cumul/1000.0
        # convention here is that for sea level data AFTER survey date, levels will be reduced by the cumulative subsidence since the survey
        # for sea level data BEFORE survey date, levels will be increased by the cumulative subsidence since 

        eustatic_levels.append(corrected_level)

    ESLR_hr_regress = scipy.stats.linregress(hours,eustatic_levels)
    ESLR_m_hr = ESLR_hr_regress[0]
    ESLR_mm_hr = ESLR_m_hr*1000

    print('gauge located @ %s' % gage_loc[site])
    print('%0.3f mm/yr \t\t: Subsidence rate (extracted from CPRA data)'                     % (gage_sub_cpra_mm_yr[site]) )
    print('%0.3f mm/yr \t\t: Gage-specific subsidence (calculated from SLR record)'           % (sub_mm_hr*365.25*24) )
    print('%0.3f mm/yr \t\t: RSLR (%d-%d) - calculated from un-corrected gage data'           % (RSLR_mm_hr*365.25*24,years[0],years[-1]) )
    print('%0.3f mm/yr \t\t: ESLR (%d-%d) - calculated from subsidence-corrected gage data'   % (ESLR_mm_hr*365.25*24,years[0],years[-1]) )
    print('%0.3f mm/yr \t\t: ESLR (1992-2023) - NOAA Satellite Altimetry'                     % GOM_ESLR_mm_yr )

    outfile = 'subsidence_adjusted/NOAA_%d_%d-%d_subsidence_adjusted.csv' % (site,years[0],years[-1])
    if site == 73813498:
        obs_file = 'USGS_%d_%d-%d_subsidence_adjusted.csv' % (site,years[0],years[-1])
    datum_shift = gage_survey[site]['NAVD88_rel_MTL_m']
    print('%0.4f m \t\t: NAVD88 datum relative to MTL @ %s' % (datum_shift,site))
    with open(outfile,mode='w') as of:
        of.write('Observed tidal data for NOAA gage: %d, %s\n' % (site,gage_names[site]))
        of.write('datetime (GMT),elapsed hours,elapsed hours since survey,relative sea level (m NAVD88),eustatic sea level (m NAVD88),data flag (0=observed;1=filled from predicted)\n')
        for i in range(0,len(hours)):
            of.write('%04d-%02d-%02d %02d:00,%d,%d,%0.4f,%0.4f,%d\n' %(dates[i].year,dates[i].month,dates[i].day,dates[i].hour,hours[i],hours_since_survey[i],relative_levels[i]-datum_shift,eustatic_levels[i]-datum_shift,flags[i]))

    
    
