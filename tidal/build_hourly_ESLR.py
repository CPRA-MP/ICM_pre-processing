import numpy as np
import datetime as dt


eslr_2010_2020_mm = 4.9471*(2020-2010)     #GOMA ESLR rate = 4.971 mm/yr (calculated in Feb 2025 from seasonally-adjusted NOAA satellite altimetery data - see Scenarios ESLR spreadsheet)
eslr_offset = eslr_2010_2020_mm / 1000

s = 'ssp2-4.5-50th'
ESLR_column = -2


startyear = 2025
endyear = 2079

startday = dt.datetime(startyear,1,1)
endday = dt.datetime(endyear+1,1,1)
years = range(startyear,endyear+1)

future_hours = []
for h in (startday + dt.timedelta(d/24) for d in range(0,(endday-startday).days*24)):
	future_hours.append(h)

tidal_file =    'MP2023_2010_tidal_WSE.csv'
ESLR_file  =    'MP29_ESLR_curves.csv'


file2write =    'MP29_%s_TideData.csv' % s


# read in hourly tidal data for a given year
print('reading in hourly tidal data for representative year from %s'  % tidal_file)
tidal = np.genfromtxt(tidal_file, skip_header = 1, delimiter=',',dtype=str)
tidal_mdh = {}

for row in tidal:
    timedate =  row[1]
    data =      row[2:]
    md =        timedate[4:8]               # MMDD
    h =         timedate[9:11]              # HH
    mdh = '%s%s' % (md,h)

    tidal_mdh[mdh] = data
    

# check if annual tidal data has leap day data, if not repeat Feb 28 for Feb 29
print('checking for leap day data')
if '022901' not in tidal_mdh.keys():
    for h in range(0,25):
        mdh = '0229%02d' % h
        mdh2fill = '0228%02d' % h
        try:            # using try/except here to cover cases for both midnight as 00:00 and 24:00
            tidal_mdh[mdh] = tidal_mdh[mdh2fill]
        except:
            _ = 'mdh2fill not in tidal_mdh'
        
# read in 50 year timeseries ESLR curves
print('reading in ESLR curves')
eslr_yearly_arr = np.genfromtxt(ESLR_file, skip_header = 1, delimiter=',',usecols=[0,ESLR_column],dtype=str)
ESLRend = float(eslr_yearly_arr[-1][1])  # this is the last timestep ESLR summarzing the total range of ESLR

eslr_yearly = {}
for row in eslr_yearly_arr:
    yr = int(row[0])
    val = float(row[1])
    eslr_yearly[yr] = val
             




print('writing output file: %s' % file2write)
with open(file2write,mode='w') as outfile:
    outfile.write('ESLR=%0.2fm (%04d-%04d) - Date Time,ICM Tide Gage 1 - NOAA 8735180 Dauphin Island Mobile Bay AL (adjusted by x amplify),ICM Tide Gage 2 - NOAA 8760922 Pilots Station East Southwest Pass LA,ICM Tide Gage 3 - NOAA 8761724 Grand Isle LA,ICM Tide Gage 4 - USGS 073813498 Caillou Bay LA-zwRev,ICM Tide Gage 5 - NOAA 8768094 Calcasieu Pass LA,ICM Tide Gage 6 - NOAA 8770570 Sabine Pass TX\n' % (ESLRend,startyear,endyear) )
    for ts in future_hours:
        m = ts.month
        d = ts.day
        y = ts.year
        h = ts.hour
        mdh = '%02d%02d%02d' % (m,d,h)

        eslr_val = eslr_yearly[y]
    
        tidal_data = tidal_mdh[mdh]
        line2write = '%04d%02d%02d %02d:00:00' % (y,m,d,h)

        for gage_val in tidal_data:
            mwl =  float(gage_val) + eslr_val + eslr_offset
            line2write = '%s,%0.6f' % (line2write,mwl)
            
        outfile.write('%s\n' % line2write)
        
