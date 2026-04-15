import numpy as np

old_file = 'MP29_future_conditions_AORCprecip_uplandQ_2025_2079_ssp2-4.5_old_compartments.csv'
new_file = 'MP29_future_conditions_AORCprecip_uplandQ_2025_2079_ssp2-4.5.csv'

remap_file = 'precip_compartment_remap_April2026.csv'

remap = {}
remap_arr = np.genfromtxt(remap_file,dtype='int',delimiter=',',usecols=[0,4],skip_header=1)

for newcomp in remap_arr:
    remap[newcomp[0]] = newcomp[1]

with open(old_file,mode='r') as old:
    with open(new_file,mode='w') as new:
        header = 'yyyy-mm-dd'
        for newcomp in remap.keys():
            header = '%s,%d' % (header,newcomp)
        _ = new.write('%s\n' % header)
        
        nl = 0
        for line in old:
            if nl > 0: # skip header
                old_row = line.split(',')
                newline = old_row[0]
                for newcomp in remap.keys():
                    oldcomp = remap[newcomp]
                    oldrain = old_row[oldcomp].strip()  # look up old column of rain for the remapped comp and remove any new line characters
                    newline = '%s,%s' % (newline,oldrain)
                _ = new.write('%s\n' % newline)
                
            nl += 1
            

                
