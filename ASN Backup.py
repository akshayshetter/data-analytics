# %%
import pandas as pd 
import numpy as np
import pygsheets

# %%
gc = pygsheets.authorize(service_file ='C:/Users/Dell/Documents/Python Scripts/Note/test-api-403913-222895678b66.json')

#%% 
# open ogv daily req sheet

Ogv_asn = gc.open_by_key('19LOa1geQijLD9AiBPAOGOvxsyQ_Mat6JFymUthI24AU') # OGV po request sheet

daily_asn = Ogv_asn.worksheet_by_title('ASN Report')

daily_asn_df = daily_asn.get_as_df(Start='B1',end='I')

daily_asn = daily_asn_df[daily_asn_df['Vendor Name']!='']

req_col = [ 'City',	'Date',	'Vendor ID',	'Vendor Name',	'Facility',	'Delivery Date',	'Vehicle Status',	'Reason Code']

daily_req_df2 = daily_asn_df[req_col]

#%%
print(daily_asn_df)
#%%
print(daily_req_df2)

#%%
#%%
daily_backup = Ogv_asn.worksheet_by_title('ASN Backup')

backup_df = daily_backup.get_as_df(start='A1',end='H')

backup_df = pd.concat([backup_df,daily_asn_df],axis=0)

daily_backup.set_dataframe(backup_df,start='A1',end='U')

#%%
Ogv_asn = gc.open_by_key('19LOa1geQijLD9AiBPAOGOvxsyQ_Mat6JFymUthI24AU') # OGV po request sheet

daily_asn = Ogv_asn.worksheet_by_title('ASN Report')
#%%
daily_asn.clear(start='H2',end='H1000')

daily_asn.clear(start='I2',end='I1000')

daily_asn.clear(start='J2',end='J1000')

#daily_backup = Ogv_req.worksheet_by_title('Backup final')

#backup_df = daily_backup.get_as_df()
#%%
#backup_df = pd.concat([backup_df,daily_req_df2],axis=0)

#%%

#daily_backup.set_dataframe(backup_df,start='B1',end='J')

# %%
