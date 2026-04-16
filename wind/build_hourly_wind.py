import numpy as np
import datetime as dt


eslr_2010_2020_mm = 4.9471*(2020-2010)     #GOMA ESLR rate = 4.971 mm/yr (calculated in Feb 2025 from seasonally-adjusted NOAA satellite altimetery data - see Scenarios ESLR spreadsheet)
eslr_offset = eslr_2010_2020_mm / 1000

s = 'ssp2-4.5'
startyear = 2025
endyear = 2079

input_file_X =    'WindVectorsX_2010.csv'
input_file_Y =    'WindVectorsY_2010.csv'

input_file_dt = 3                           # hourly timestep of the observed wind data being used

file2writeX =    'MP29_%s_WindVectorsX.csv' % s
file2writeY =    'MP29_%s_WindVectorsY.csv' % s

startday = dt.datetime(startyear,1,1)
endday = dt.datetime(endyear+1,1,1)
years = range(startyear,endyear+1)
ndt = int(24/input_file_dt)                 # number of timesteps per day

future_dt = []
for h in (startday + dt.timedelta(d/ndt) for d in range(0,(endday-startday).days*ndt)):
	future_dt.append(h)




# read in 3-hourly wind data for a given year
print('reading in hourly wind data for representative year from %s & %s'  % (input_file_X,input_file_Y))
with open(input_file_X,mode='r') as inp:
	wind_hdr = inp.readline()           # save header line for writing output file
	
windX = np.genfromtxt(input_file_X, skip_header = 1, delimiter=',',dtype=str)
windY = np.genfromtxt(input_file_Y, skip_header = 1, delimiter=',',dtype=str)

windX_mdh = {}
windY_mdh = {}    

for row in windX:                           # timedate format in wind file is 'YYYYMMDD HH:00:00'
    timedate =  row[0]
    data =      row[1:]
    md =        timedate[4:8]               # MMDD
    h =         timedate[9:11]              # HH
    mdh = '%s%s' % (md,h)

    windX_mdh[mdh] = data

for row in windY:
    timedate =  row[0]
    data =      row[1:]
    md =        timedate[4:8]               # MMDD
    h =         timedate[9:11]              # HH
    mdh = '%s%s' % (md,h)

    windY_mdh[mdh] = data
    
   
# check if annual wind data has leap day data, if not repeat Feb 28 for Feb 29
print('checking for leap day data')
if '022901' not in windX_mdh.keys():
    for h in range(0,25):
        mdh = '0229%02d' % h
        mdh2fill = '0228%02d' % h
        try:            # using try/except here to cover cases for both midnight as 00:00 and 24:00
            windX_mdh[mdh] = windX_mdh[mdh2fill]
        except:
            _ = 'mdh2fill not in tidal_mdh'        

if '022901' not in windY_mdh.keys():
    for h in range(0,25):
        mdh = '0229%02d' % h
        mdh2fill = '0228%02d' % h
        try:            # using try/except here to cover cases for both midnight as 00:00 and 24:00
            windY_mdh[mdh] = windY_mdh[mdh2fill]
        except:
            _ = 'mdh2fill not in tidal_mdh'


print('writing output file: %s & %s ' % (file2writeX,file2writeY))
with open(file2writeX,mode='w') as outfileX:
    with open(file2writeY,mode='w') as outfileY:

        _ = outfileX.write(wind_hdr)
        _ = outfileY.write(wind_hdr)
        
        for ts in future_dt:
            m = ts.month
            d = ts.day
            y = ts.year
            h = ts.hour
            mdh = '%02d%02d%02d' % (m,d,h)

            line2writeX = '%04d%02d%02d %02d:00:00' % (y,m,d,h)
            for col in windX_mdh[mdh]:
                line2writeX = '%s,%s' % (line2writeX,col)
            _= outfileX.write('%s\n' % line2writeX)

            line2writeY = '%04d%02d%02d %02d:00:00' % (y,m,d,h)
            for col in windY_mdh[mdh]:
                line2writeY = '%s,%s' % (line2writeY,col)
            _= outfileY.write('%s\n' % line2writeY)
