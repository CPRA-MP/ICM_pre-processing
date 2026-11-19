########################################################################
import pandas as pd
import time
import numpy as np
import math
from datetime import datetime
import os
import matplotlib
import matplotlib.pyplot as plt
import scipy.stats

ws = "U:\\MPSubsidence\\Reevaluating Subsidence\\Task 2 Reanalyzing shallow subsidence\\Shallow_Sub\\explanatory_variables_analysis"
F_rates = "U:\\MPSubsidence\Reevaluating Subsidence\\Task 2 Reanalyzing shallow subsidence\\CRMS_DATA\All_combined_edits.csv"
########################################################################
########################################################################
F_CD = os.path.join(ws, "CRMS Chenier_Delta.xlsx")
CD = pd.read_excel(F_CD)
rates = pd.read_csv(F_rates)
CD_rates = pd.merge(rates,CD,on="SiteID",how="outer")
########################################################################
Chen = [np.nan]*len(CD_rates['Accretion_Rate (mm/yr)'])
Delt = [np.nan]*len(CD_rates['Accretion_Rate (mm/yr)'])
for i in range(len(CD_rates['Accretion_Rate (mm/yr)'])):
    if CD_rates['Chenier/Deltaic'][i] == "Chenier":
        Chen[i] = CD_rates['Accretion_Rate (mm/yr)'][i]
    if CD_rates['Chenier/Deltaic'][i] == "Deltaic":
        Delt[i] = CD_rates['Accretion_Rate (mm/yr)'][i]

Chen = [x for x in Chen if (math.isnan(x) == False)]
Delt = [x for x in Delt if (math.isnan(x) == False)]

fig0 = plt.figure()
ax = fig0.add_subplot(111)
plt.boxplot([Chen,Delt])
ax.set_xticklabels(['Chenier','Deltaic'], rotation = 45)
ax.set_title("VA (mm/yr)")
#plt.show()
########################################################################
Chen = [np.nan]*len(CD_rates['SEC_Rate (mm/yr)'])
Delt = [np.nan]*len(CD_rates['SEC_Rate (mm/yr)'])
for i in range(len(CD_rates['SEC_Rate (mm/yr)'])):
    if CD_rates['Chenier/Deltaic'][i] == "Chenier":
        Chen[i] = CD_rates['Accretion_Rate (mm/yr)'][i]
    if CD_rates['Chenier/Deltaic'][i] == "Deltaic":
        Delt[i] = CD_rates['SEC_Rate (mm/yr)'][i]

Chen = [x for x in Chen if (math.isnan(x) == False)]
Delt = [x for x in Delt if (math.isnan(x) == False)]

fig1 = plt.figure()
ax = fig1.add_subplot(111)
plt.boxplot([Chen,Delt])
ax.set_xticklabels(['Chenier','Deltaic'], rotation = 45)
ax.set_title("SEC(mm/yr)")
#plt.show()
########################################################################
########################################################################
F_z = os.path.join(ws, "Copy of 2021 G12b Marsh Elevation_All points.xlsx")
elev = pd.read_excel(F_z)
########################################################################
uniqCRMS = pd.unique(elev["Site ID"])
elevsyn = pd.DataFrame(columns = ["SiteID", "Avg z", "Avg z surf", "Avg z crown", "Avg H2O depth", "Avg H2O surf", "Avg H2O crown"], index = range(len(uniqCRMS)))
i=0
for site in uniqCRMS:
    elevsyn["SiteID"][i] = site
    subtable = elev[elev["Site ID"] == site]
    elevsyn["Avg z"][i] = sum(subtable["Marsh Elevation (ft, G12b)"])/len(subtable["Marsh Elevation (ft, G12b)"])
    elevsyn["Avg H2O depth"][i] = sum(subtable["Water depth (ft)"])/len(subtable["Water depth (ft)"])
    
    subtablecro = subtable[subtable["Surface Type (Surface / Crown)"] == "Crown"]
    if len(subtablecro) != 0:
        elevsyn["Avg z crown"][i] = sum(subtablecro["Marsh Elevation (ft, G12b)"])/len(subtablecro["Marsh Elevation (ft, G12b)"])
        elevsyn["Avg H2O crown"][i] = sum(subtablecro["Water depth (ft)"])/len(subtablecro["Water depth (ft)"])
    else:
        elevsyn["Avg z crown"][i] = np.nan
        elevsyn["Avg H2O crown"][i] = np.nan

    subtablesur = subtable[subtable["Surface Type (Surface / Crown)"] == "Surface"]
    if len(subtablesur) != 0:
        elevsyn["Avg z surf"][i] = sum(subtablesur["Marsh Elevation (ft, G12b)"])/len(subtablesur["Marsh Elevation (ft, G12b)"])
        elevsyn["Avg H2O surf"][i] = sum(subtablesur["Water depth (ft)"])/len(subtablesur["Water depth (ft)"])
    else:
        elevsyn["Avg z surf"][i] = np.nan
        elevsyn["Avg H2O surf"][i] = np.nan
    i = i+1
##########################################################################
mergeCRMS = pd.merge(CD_rates,elevsyn,on="SiteID",how="outer")

fig2 = plt.figure()
ax = fig2.add_subplot(111)
ax.set_title("Rates vs Avg z")
plt.scatter(mergeCRMS["Avg z"],mergeCRMS['Accretion_Rate (mm/yr)'])
df = mergeCRMS[["Avg z",'Accretion_Rate (mm/yr)']].dropna()
x = df["Avg z"].to_numpy(dtype = np.float64)
y = df['Accretion_Rate (mm/yr)'].to_numpy(dtype = np.float64)
m, b, r_value, p_value, stderr = scipy.stats.linregress(x,y)
plt.plot(x,x*m + b)
VALOBF = 'R^2 = ' + str("{:.3f}".format(r_value**2))

plt.scatter(mergeCRMS["Avg z"],mergeCRMS['SEC_Rate (mm/yr)'])
df = mergeCRMS[["Avg z",'SEC_Rate (mm/yr)']].dropna()
x = df["Avg z"].to_numpy(dtype = np.float64)
y = df['SEC_Rate (mm/yr)'].to_numpy(dtype = np.float64)
m, b, r_value, p_value, stderr = scipy.stats.linregress(x,y)
plt.plot(x,x*m + b)
SECLOBF = 'R^2 = ' + str("{:.3f}".format(r_value**2))

ax.legend(["VA",VALOBF, "SEC",SECLOBF])
ax.set_ylabel('Rate (mm/yr)')
ax.set_xlabel('Average Elevation (NAVD88[m])')
#plt.show()

fig3 = plt.figure()
ax = fig3.add_subplot(111)
ax.set_title("Rates vs Avg z crown")
plt.scatter(mergeCRMS["Avg z crown"],mergeCRMS['Accretion_Rate (mm/yr)'])
df = mergeCRMS[["Avg z crown",'Accretion_Rate (mm/yr)']].dropna()
x = df["Avg z crown"].to_numpy(dtype = np.float64)
y = df['Accretion_Rate (mm/yr)'].to_numpy(dtype = np.float64)
m, b, r_value, p_value, stderr = scipy.stats.linregress(x,y)
plt.plot(x,x*m + b)
VALOBF = 'R^2 = ' + str("{:.3f}".format(r_value**2))

plt.scatter(mergeCRMS["Avg z crown"],mergeCRMS['SEC_Rate (mm/yr)'])
df = mergeCRMS[["Avg z crown",'SEC_Rate (mm/yr)']].dropna()
x = df["Avg z crown"].to_numpy(dtype = np.float64)
y = df['SEC_Rate (mm/yr)'].to_numpy(dtype = np.float64)
m, b, r_value, p_value, stderr = scipy.stats.linregress(x,y)
plt.plot(x,x*m + b)
SECLOBF = 'R^2 = ' + str("{:.3f}".format(r_value**2))

ax.legend(["VA",VALOBF, "SEC",SECLOBF])
ax.set_ylabel('Rate (mm/yr)')
ax.set_xlabel('Average Crown Elevation (NAVD88[m])')
#plt.show()

fig4 = plt.figure()
ax = fig4.add_subplot(111)
ax.set_title("Rates vs Avg z surf")
plt.scatter(mergeCRMS["Avg z surf"],mergeCRMS['Accretion_Rate (mm/yr)'])
df = mergeCRMS[["Avg z surf",'Accretion_Rate (mm/yr)']].dropna()
x = df["Avg z surf"].to_numpy(dtype = np.float64)
y = df['Accretion_Rate (mm/yr)'].to_numpy(dtype = np.float64)
m, b, r_value, p_value, stderr = scipy.stats.linregress(x,y)
plt.plot(x,x*m + b)
VALOBF = 'R^2 = ' + str("{:.3f}".format(r_value**2))

plt.scatter(mergeCRMS["Avg z surf"],mergeCRMS['SEC_Rate (mm/yr)'])
df = mergeCRMS[["Avg z surf",'SEC_Rate (mm/yr)']].dropna()
x = df["Avg z surf"].to_numpy(dtype = np.float64)
y = df['SEC_Rate (mm/yr)'].to_numpy(dtype = np.float64)
m, b, r_value, p_value, stderr = scipy.stats.linregress(x,y)
plt.plot(x,x*m + b)
SECLOBF = 'R^2 = ' + str("{:.3f}".format(r_value**2))

ax.legend(["VA",VALOBF, "SEC",SECLOBF])
ax.set_ylabel('Rate (mm/yr)')
ax.set_xlabel('Average Surface Elevation (NAVD88[m])')
#plt.show()

fig5 = plt.figure()
ax = fig5.add_subplot(111)
ax.set_title("Rates vs Avg H2O depth")
plt.scatter(mergeCRMS["Avg H2O depth"],mergeCRMS['Accretion_Rate (mm/yr)'])
df = mergeCRMS[["Avg H2O depth",'Accretion_Rate (mm/yr)']].dropna()
x = df["Avg H2O depth"].to_numpy(dtype = np.float64)
y = df['Accretion_Rate (mm/yr)'].to_numpy(dtype = np.float64)
m, b, r_value, p_value, stderr = scipy.stats.linregress(x,y)
plt.plot(x,x*m + b)
VALOBF = 'R^2 = ' + str("{:.3f}".format(r_value**2))

plt.scatter(mergeCRMS["Avg H2O depth"],mergeCRMS['SEC_Rate (mm/yr)'])
df = mergeCRMS[["Avg H2O depth",'SEC_Rate (mm/yr)']].dropna()
x = df["Avg H2O depth"].to_numpy(dtype = np.float64)
y = df['SEC_Rate (mm/yr)'].to_numpy(dtype = np.float64)
m, b, r_value, p_value, stderr = scipy.stats.linregress(x,y)
plt.plot(x,x*m + b)
SECLOBF = 'R^2 = ' + str("{:.3f}".format(r_value**2))

ax.legend(["VA",VALOBF, "SEC",SECLOBF])
ax.set_ylabel('Rate (mm/yr)')
ax.set_xlabel('Average Water Depth [m]')
#plt.show()

fig6 = plt.figure()
ax = fig6.add_subplot(111)
ax.set_title("Rates vs Avg H2O surf")
plt.scatter(mergeCRMS["Avg H2O surf"],mergeCRMS['Accretion_Rate (mm/yr)'])
df = mergeCRMS[["Avg H2O surf",'Accretion_Rate (mm/yr)']].dropna()
x = df["Avg H2O surf"].to_numpy(dtype = np.float64)
y = df['Accretion_Rate (mm/yr)'].to_numpy(dtype = np.float64)
m, b, r_value, p_value, stderr = scipy.stats.linregress(x,y)
plt.plot(x,x*m + b)
VALOBF = 'R^2 = ' + str("{:.3f}".format(r_value**2))

plt.scatter(mergeCRMS["Avg H2O surf"],mergeCRMS['SEC_Rate (mm/yr)'],)
df = mergeCRMS[["Avg H2O surf",'SEC_Rate (mm/yr)']].dropna()
x = df["Avg H2O surf"].to_numpy(dtype = np.float64)
y = df['SEC_Rate (mm/yr)'].to_numpy(dtype = np.float64)
m, b, r_value, p_value, stderr = scipy.stats.linregress(x,y)
plt.plot(x,x*m + b)
SECLOBF = 'R^2 = ' + str("{:.3f}".format(r_value**2))

ax.legend(["VA",VALOBF, "SEC",SECLOBF])
ax.set_ylabel('Rate (mm/yr)')
ax.set_xlabel('Average Surface Water Depth [m]')
#plt.show()

fig7 = plt.figure()
ax = fig7.add_subplot(111)
ax.set_title("Rates vs Avg H2O crown")
plt.scatter(mergeCRMS["Avg H2O crown"],mergeCRMS['Accretion_Rate (mm/yr)'])
df = mergeCRMS[["Avg H2O crown",'Accretion_Rate (mm/yr)']].dropna()
x = df["Avg H2O crown"].to_numpy(dtype = np.float64)
y = df['Accretion_Rate (mm/yr)'].to_numpy(dtype = np.float64)
m, b, r_value, p_value, stderr = scipy.stats.linregress(x,y)
plt.plot(x,x*m + b)
VALOBF = 'R^2 = ' + str("{:.3f}".format(r_value**2))

plt.scatter(mergeCRMS["Avg H2O crown"],mergeCRMS['SEC_Rate (mm/yr)'])
df = mergeCRMS[["Avg H2O crown",'SEC_Rate (mm/yr)']].dropna()
x = df["Avg H2O crown"].to_numpy(dtype = np.float64)
y = df['SEC_Rate (mm/yr)'].to_numpy(dtype = np.float64)
m, b, r_value, p_value, stderr = scipy.stats.linregress(x,y)
plt.plot(x,x*m + b)
SECLOBF = 'R^2 = ' + str("{:.3f}".format(r_value**2))

ax.legend(["VA",VALOBF, "SEC",SECLOBF])
ax.set_ylabel('Rate (mm/yr)')
ax.set_xlabel('Average Crown Water Depth [m]')
#plt.show()
#############################################################################
F_marshclass = os.path.join(ws, "site marsh class assignments.xlsx")
marshclass = pd.read_excel(F_marshclass)
mergeCRMS2 = pd.merge(mergeCRMS, marshclass, on="SiteID", how="outer")
uniqmarshtype = pd.unique(mergeCRMS2["Site Marsh Class across Years"])
submarshA = [pd.DataFrame()]*len(uniqmarshtype)
submarshS = [pd.DataFrame()]*len(uniqmarshtype)
i = 0
for marsh in uniqmarshtype:
    submarshA[i] = [x for x in mergeCRMS2['Accretion_Rate (mm/yr)'][mergeCRMS2["Site Marsh Class across Years"] == marsh] if (math.isnan(x) == False)]
    submarshS[i] = [x for x in mergeCRMS2['SEC_Rate (mm/yr)'][mergeCRMS2["Site Marsh Class across Years"] == marsh] if (math.isnan(x) == False)]
    i=i+1
fig8 = plt.figure()
ax = fig8.add_subplot(111)
plt.boxplot(submarshA[1:])
ax.set_title("VA by Marsh Type")
ax.set_xticklabels(uniqmarshtype[1:])
ax.set_ylabel('Rate (mm/yr)')
#plt.show()
fig9 = plt.figure()
ax = fig9.add_subplot(111)
plt.boxplot(submarshS[1:])
ax.set_title("SEC by Marsh Type")
ax.set_xticklabels(uniqmarshtype[1:])
ax.set_ylabel('Rate (mm/yr)')
#plt.show()
#############################################################################
F_ffibs = os.path.join(ws, "FFIBS Score by site year.xlsx")
ffibs = pd.read_excel(F_ffibs)
uniqsite = pd.unique(ffibs["Site"])
ffib = pd.DataFrame(columns = ["SiteID", "AvgFFIBS"], index = range(len(uniqsite)))

i = 0
for site in uniqsite:
    ffib["SiteID"][i] = site
    subtable = ffibs[ffibs["Site"] == site]
    ffib["AvgFFIBS"][i] = sum(subtable["FFIBS_Score"])/len(subtable["FFIBS_Score"])
    i = i+1

mergeCRMS3 = pd.merge(mergeCRMS2, ffib, on="SiteID", how="outer")
    
fig10 = plt.figure()
ax = fig10.add_subplot(111)
ax.set_title("Rates vs FFIBS")
plt.scatter(mergeCRMS3["AvgFFIBS"],mergeCRMS3['Accretion_Rate (mm/yr)'],)
df = mergeCRMS3[["AvgFFIBS",'Accretion_Rate (mm/yr)']].dropna()
x = df["AvgFFIBS"].to_numpy(dtype = np.float64)
y = df['Accretion_Rate (mm/yr)'].to_numpy(dtype = np.float64)
m, b, r_value, p_value, stderr = scipy.stats.linregress(x,y)
plt.plot(x,x*m + b)
VALOBF = 'R^2 = ' + str("{:.3f}".format(r_value**2))

plt.scatter(mergeCRMS3["AvgFFIBS"],mergeCRMS3['SEC_Rate (mm/yr)'],)
df = mergeCRMS3[["AvgFFIBS",'SEC_Rate (mm/yr)']].dropna()
x = df["AvgFFIBS"].to_numpy(dtype = np.float64)
y = df['SEC_Rate (mm/yr)'].to_numpy(dtype = np.float64)
m, b, r_value, p_value, stderr = scipy.stats.linregress(x,y)
plt.plot(x,x*m + b)
SECLOBF = 'R^2 = ' + str("{:.3f}".format(r_value**2))

ax.legend(["VA",VALOBF, "SEC",SECLOBF])
ax.set_ylabel('Rate (mm/yr)')
ax.set_xlabel('Average FFIBS score')
#plt.show()
##############################################################################
##############################################################################
##############################################################################
F_soilprops = os.path.join(ws, "Copy of Site mean OM_BD by year.xlsx")
soilprops = pd.read_excel(F_soilprops)
uniqsite = pd.unique(soilprops['Site ID'])

CRMSsoil = pd.DataFrame(columns = ['SiteID','Avg_BD', 'Avg_OM', 'Avg_OD', 'Avg_TC', 'Avg_CD'],
                                     index = range(len(uniqsite)))
i = 0
for site in uniqsite:
    subtable1 = soilprops[soilprops['Site ID'] == site]
    subtable = subtable1[subtable1['Is Max Sample Year'] == 1]
    CRMSsoil['SiteID'][i] = site
    CRMSsoil['Avg_BD'][i] = sum(subtable['Mean(Bulk Density (g/cm3))'])/len(subtable['Mean(Bulk Density (g/cm3))'])
    CRMSsoil['Avg_OM'][i] = sum(subtable['Mean(Organic Matter (%))'])/len(subtable['Mean(Organic Matter (%))'])
    CRMSsoil['Avg_OD'][i] = sum(subtable['Mean(Organic Density (g/cm3))'])/len(subtable['Mean(Organic Density (g/cm3))'])
    CRMSsoil['Avg_TC'][i] = sum(subtable['Mean(Total Carbon (g/kg))'])/len(subtable['Mean(Total Carbon (g/kg))'])
    CRMSsoil['Avg_CD'][i] = sum(subtable['Mean(Carbon Density (mg/cm3))'])/len(subtable['Mean(Carbon Density (mg/cm3))'])
    i=i+1

mergeCRMS4 = pd.merge(mergeCRMS3, CRMSsoil, on="SiteID", how="outer")
#############################################################################
#############################################################################
fig11 = plt.figure()
ax = fig11.add_subplot(111)
ax.set_title("Rates vs BD")
plt.scatter(mergeCRMS4['Avg_BD'],mergeCRMS4['Accretion_Rate (mm/yr)'])
df = mergeCRMS4[['Avg_BD','Accretion_Rate (mm/yr)']].dropna()
x = df['Avg_BD'].to_numpy(dtype = np.float64)
y = df['Accretion_Rate (mm/yr)'].to_numpy(dtype = np.float64)
m, b, r_value, p_value, stderr = scipy.stats.linregress(x,y)
plt.plot(x,x*m + b)
VALOBF = 'R^2 = ' + str("{:.3f}".format(r_value**2))

plt.scatter(mergeCRMS4['Avg_BD'],mergeCRMS4['SEC_Rate (mm/yr)'])
df = mergeCRMS4[['Avg_BD','SEC_Rate (mm/yr)']].dropna()
x = df['Avg_BD'].to_numpy(dtype = np.float64)
y = df['SEC_Rate (mm/yr)'].to_numpy(dtype = np.float64)
m, b, r_value, p_value, stderr = scipy.stats.linregress(x,y)
plt.plot(x,x*m + b)
SECLOBF = 'R^2 = ' + str("{:.3f}".format(r_value**2))

ax.set_ylabel('Rate (mm/yr)')
ax.set_xlabel('Mean(Bulk Density (g/cm3))')
ax.legend(["VA",VALOBF, "SEC",SECLOBF])
#############################################################################
fig12 = plt.figure()
ax = fig12.add_subplot(111)
ax.set_title("Rates vs OM")
plt.scatter(mergeCRMS4['Avg_OM'],mergeCRMS4['Accretion_Rate (mm/yr)'],)
df = mergeCRMS4[['Avg_OM','Accretion_Rate (mm/yr)']].dropna()
x = df['Avg_OM'].to_numpy(dtype = np.float64)
y = df['Accretion_Rate (mm/yr)'].to_numpy(dtype = np.float64)
m, b, r_value, p_value, stderr = scipy.stats.linregress(x,y)
plt.plot(x,x*m + b)
VALOBF = 'R^2 = ' + str("{:.3f}".format(r_value**2))

plt.scatter(mergeCRMS4['Avg_OM'],mergeCRMS4['SEC_Rate (mm/yr)'],)
df = mergeCRMS4[['Avg_OM','SEC_Rate (mm/yr)']].dropna()
x = df['Avg_OM'].to_numpy(dtype = np.float64)
y = df['SEC_Rate (mm/yr)'].to_numpy(dtype = np.float64)
m, b, r_value, p_value, stderr = scipy.stats.linregress(x,y)
plt.plot(x,x*m + b)
SECLOBF = 'R^2 = ' + str("{:.3f}".format(r_value**2))

ax.set_ylabel('Rate (mm/yr)')
ax.set_xlabel('Mean(Organic Matter (%))')
ax.legend(["VA",VALOBF, "SEC",SECLOBF])
#############################################################################
fig13 = plt.figure()
ax = fig13.add_subplot(111)
ax.set_title("Rates vs OD")
plt.scatter(mergeCRMS4['Avg_OD'],mergeCRMS4['Accretion_Rate (mm/yr)'],)
df = mergeCRMS4[['Avg_OD','Accretion_Rate (mm/yr)']].dropna()
x = df['Avg_OD'].to_numpy(dtype = np.float64)
y = df['Accretion_Rate (mm/yr)'].to_numpy(dtype = np.float64)
m, b, r_value, p_value, stderr = scipy.stats.linregress(x,y)
plt.plot(x,x*m + b)
VALOBF = 'R^2 = ' + str("{:.3f}".format(r_value**2))

plt.scatter(mergeCRMS4['Avg_OD'],mergeCRMS4['SEC_Rate (mm/yr)'],)
df = mergeCRMS4[['Avg_OD','SEC_Rate (mm/yr)']].dropna()
x = df['Avg_OD'].to_numpy(dtype = np.float64)
y = df['SEC_Rate (mm/yr)'].to_numpy(dtype = np.float64)
m, b, r_value, p_value, stderr = scipy.stats.linregress(x,y)
plt.plot(x,x*m + b)
SECLOBF = 'R^2 = ' + str("{:.3f}".format(r_value**2))

ax.set_ylabel('Rate (mm/yr)')
ax.set_xlabel('Mean(Organic Density (g/cm3))')
ax.legend(["VA",VALOBF, "SEC",SECLOBF])
###########################################################################
fig14 = plt.figure()
ax = fig14.add_subplot(111)
ax.set_title("Rates vs TC")
plt.scatter(mergeCRMS4['Avg_TC'],mergeCRMS4['Accretion_Rate (mm/yr)'],)
df = mergeCRMS4[['Avg_TC','Accretion_Rate (mm/yr)']].dropna()
x = df['Avg_TC'].to_numpy(dtype = np.float64)
y = df['Accretion_Rate (mm/yr)'].to_numpy(dtype = np.float64)
m, b, r_value, p_value, stderr = scipy.stats.linregress(x,y)
plt.plot(x,x*m + b)
VALOBF = 'R^2 = ' + str("{:.3f}".format(r_value**2))

plt.scatter(mergeCRMS4['Avg_TC'],mergeCRMS4['SEC_Rate (mm/yr)'],)
df = mergeCRMS4[['Avg_TC','SEC_Rate (mm/yr)']].dropna()
x = df['Avg_TC'].to_numpy(dtype = np.float64)
y = df['SEC_Rate (mm/yr)'].to_numpy(dtype = np.float64)
m, b, r_value, p_value, stderr = scipy.stats.linregress(x,y)
plt.plot(x,x*m + b)
SECLOBF = 'R^2 = ' + str("{:.3f}".format(r_value**2))

ax.set_ylabel('Rate (mm/yr)')
ax.set_xlabel('Mean(Total Carbon (g/kg))')
ax.legend(["VA",VALOBF, "SEC",SECLOBF])
############################################################################
fig15 = plt.figure()
ax = fig15.add_subplot(111)
ax.set_title("Rates vs CD")

plt.scatter(mergeCRMS4['Avg_CD'],mergeCRMS4['Accretion_Rate (mm/yr)'])
df = mergeCRMS4[['Avg_CD','Accretion_Rate (mm/yr)']].dropna()
x = df['Avg_CD'].to_numpy(dtype = np.float64)
y = df['Accretion_Rate (mm/yr)'].to_numpy(dtype = np.float64)
m, b, r_value, p_value, stderr = scipy.stats.linregress(x,y)
plt.plot(x,x*m + b)
VALOBF = 'R^2 = ' + str("{:.3f}".format(r_value**2))

plt.scatter(mergeCRMS4['Avg_CD'],mergeCRMS4['SEC_Rate (mm/yr)'])
df = mergeCRMS4[['Avg_CD','SEC_Rate (mm/yr)']].dropna()
x = df['Avg_CD'].to_numpy(dtype = np.float64)
y = df['SEC_Rate (mm/yr)'].to_numpy(dtype = np.float64)
m, b, r_value, p_value, stderr = scipy.stats.linregress(x,y)
plt.plot(x,x*m + b)
SECLOBF = 'R^2 = ' + str("{:.3f}".format(r_value**2))

ax.set_ylabel('Rate (mm/yr)')
ax.set_xlabel('Mean(Carbon Density (mg/cm3))')
ax.legend(["VA",VALOBF, "SEC",SECLOBF])


##depths = np.append(pd.unique(soilprops['Sample Depth (cm)']),'Full core')
##columns1 = ['Avg_BD', 'Avg_OM', 'Avg_OD', 'Avg_TC', 'Avg_CD']
##for depth in depths:
##    if depth == depths[0]:
##        columns2 = [sup + depth for sup in columns1]
##    else:
##        columns2a = [sup + depth for sup in columns1]
##        columns2 = columns2 + columns2a

##
##
##for site in uniqsite:
##    subtable = soilprops[soilprops['Site ID'] == site]
##    tables = np.append(pd.unique(subtable['Sample Depth (cm)']),'Full core')   
##    #CRMSsoil['SiteID'] = site
##    tablin = [pd.DataFrame()]*len(tables)
##    i=0
##    for table in tables:
##        if table == 'Full core':
##            instable = pd.DataFrame(columns = ['Depth','Avg_BD', 'Avg_OM', 'Avg_OD', 'Avg_TC', 'Avg_CD'],
##                                     index = [0])
##            instable['Depth'] = table
##            instable['Avg_BD'] = sum(subtable['Mean(Bulk Density (g/cm3))'])/len(subtable['Mean(Bulk Density (g/cm3))'])
##            instable['Avg_OM'] = sum(subtable['Mean(Organic Matter (%))'])/len(subtable['Mean(Organic Matter (%))'])
##            instable['Avg_OD'] = sum(subtable['Mean(Organic Density (g/cm3))'])/len(subtable['Mean(Organic Density (g/cm3))'])
##            instable['Avg_TC'] = sum(subtable['Mean(Total Carbon (g/kg))'])/len(subtable['Mean(Total Carbon (g/kg))'])
##            instable['Avg_CD'] = sum(subtable['Mean(Carbon Density (mg/cm3))'])/len(subtable['Mean(Carbon Density (mg/cm3))'])
##
##            
##        else:
##            instable = pd.DataFrame(columns = ['Avg_BD', 'Avg_OM', 'Avg_OD', 'Avg_TC', 'Avg_CD'],
##                                     index = [0])
##            subtable2 = subtable[subtable['Sample Depth (cm)'] == table]
##            instable['Depth'] = table
##            instable['Avg_BD'] = sum(subtable2['Mean(Bulk Density (g/cm3))'])/len(subtable2['Mean(Bulk Density (g/cm3))'])
##            instable['Avg_OM'] = sum(subtable2['Mean(Organic Matter (%))'])/len(subtable2['Mean(Organic Matter (%))'])
##            instable['Avg_OD'] = sum(subtable2['Mean(Organic Density (g/cm3))'])/len(subtable2['Mean(Organic Density (g/cm3))'])
##            instable['Avg_TC'] = sum(subtable2['Mean(Total Carbon (g/kg))'])/len(subtable2['Mean(Total Carbon (g/kg))'])
##            instable['Avg_CD'] = sum(subtable2['Mean(Carbon Density (mg/cm3))'])/len(subtable2['Mean(Carbon Density (mg/cm3))'])
##        tablin[i] = instable
##        i=i+1
##    tablin.insert(0,site)
##    if site == uniqsite[0]:
##        CRMSsoil = tablin
##    else:
##        CRMSsoil = CRMSsoil.append(tablin)
##
##    print(CRMSsoil)
##
##            
##            
##
#######################################################################
#######################################################################


mergeCRMS4.to_excel('test.xlsx')
from matplotlib.backends.backend_pdf import PdfPages
figs = [fig0, fig1, fig2, fig3, fig4, fig5, fig6, fig7, fig8, fig9, fig10, fig11, fig12, fig13, fig14, fig15]
with PdfPages(os.path.join(ws, 'test.pdf')) as pdf:
    for fig in figs:
        pdf.savefig(fig, bbox_inches = 'tight')
        matplotlib.pyplot.close(fig)



F_soilfact = "U:\\MPSubsidence\\Reevaluating Subsidence\\Task 2 Reanalyzing shallow subsidence\\Shallow_Sub\\SoilFactor_Rates.xls"
SF = pd.read_excel(F_soilfact)
mergeCRMS5 = pd.merge(mergeCRMS4, SF, on="SiteID", how="outer")

feilds2plt = ["Avg H2O depth","Avg H2O surf","Avg H2O crown","Avg z","Avg z crown","Avg z surf","AvgFFIBS",'Avg_TC','Avg_CD','Avg_OD','Avg_OM','Avg_BD']
i=0
figSF = [plt.figure()]*(len(feilds2plt)+2)
for pltfield in feilds2plt:
    figSF[i] = plt.figure()
    ax = figSF[i].add_subplot(111)
    ax.set_title(pltfield)
    df = mergeCRMS5[[pltfield,'SCE_CRMS.SCEmmyr']].dropna()
    plt.scatter(df[pltfield],df['SCE_CRMS.SCEmmyr'])
    x = df[pltfield].to_numpy(dtype = np.float64)
    y = df['SCE_CRMS.SCEmmyr'].to_numpy(dtype = np.float64)
    m, b, r_value, p_value, stderr = scipy.stats.linregress(x,y)
    plt.plot(x,x*m + b)
    SCELOBF = 'R^2 = ' + str("{:.3f}".format(r_value**2))
    ax.set_ylabel('Rate (mm/yr)')
    ax.set_xlabel(pltfield)
    ax.legend(["Soil Factor",SCELOBF])
    i=i+1

Chen = [np.nan]*len(mergeCRMS5['SCE_CRMS.SCEmmyr'])
Delt = [np.nan]*len(mergeCRMS5['SCE_CRMS.SCEmmyr'])
for ii in range(len(mergeCRMS5['SCE_CRMS.SCEmmyr'])):
    if mergeCRMS5['Chenier/Deltaic'][ii] == "Chenier":
        Chen[ii] = mergeCRMS5['SCE_CRMS.SCEmmyr'][ii]
    if mergeCRMS5['Chenier/Deltaic'][ii] == "Deltaic":
        Delt[ii] = mergeCRMS5['SCE_CRMS.SCEmmyr'][ii]

Chen = [x for x in Chen if (math.isnan(x) == False)]
Delt = [x for x in Delt if (math.isnan(x) == False)]

figSF[i] = plt.figure()
ax = figSF[i].add_subplot(111)
plt.boxplot([Chen,Delt])
ax.set_xticklabels(['Chenier','Deltaic'], rotation = 45)
ax.set_title("SF (mm/yr)")
i=i+1

uniqmarshtype = pd.unique(mergeCRMS5["Site Marsh Class across Years"])
submarshA = [pd.DataFrame()]*len(uniqmarshtype)
ii = 0
for marsh in uniqmarshtype:
    submarshA[ii] = [x for x in mergeCRMS5['SCE_CRMS.SCEmmyr'][mergeCRMS5["Site Marsh Class across Years"] == marsh] if (math.isnan(x) == False)]
    ii=ii+1

figSF[i] = plt.figure()
ax = figSF[i].add_subplot(111)
plt.boxplot(submarshA[1:])
ax.set_title("SF by Marsh Type")
ax.set_xticklabels(uniqmarshtype[1:])
ax.set_ylabel('Rate (mm/yr)')


with PdfPages(os.path.join(ws, 'SF.pdf')) as pdf:
    for fig in figSF:
        pdf.savefig(fig, bbox_inches = 'tight')
              
mergeCRMS5.to_excel('SF.xlsx')















            
