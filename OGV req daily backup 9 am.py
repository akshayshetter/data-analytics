# %%
import pandas as pd 
import numpy as np
import pygsheets

# %%
gc = pygsheets.authorize(service_file ='C:/Users/Dell/Documents/Python Scripts/Note/test-api-403913-222895678b66.json')

#%% 
# open ogv daily req sheet

Ogv_req = gc.open_by_key('1_c2QsdGeJbJEvMO4kRAjDvWxfuZqkdE4LnLM1jF9Rfw') # OGV planning sheet

daily_req = Ogv_req.worksheet_by_title('Daily req')

daily_req_df = daily_req.get_as_df()

daily_req_df = daily_req_df[daily_req_df['Title']!='']

req_col = [ 'City',	'JPIN',	'Title',	'Current Inventory',	'DRR',	'days of cover'	,'Inventory Norm',	'System Suggested Cases', 'City-CITY Id-JPIN']

daily_req_df2 = daily_req_df[req_col]

#%%
print(daily_req_df)
#%%
print(daily_req_df2)

#%%
#%%
daily_backup = Ogv_req.worksheet_by_title('Backup')

backup_df = daily_backup.get_as_df()

backup_df = pd.concat([backup_df,daily_req_df],axis=0)

daily_backup.set_dataframe(backup_df,start='A1')

#%%
#daily_backup = Ogv_req.worksheet_by_title('Backup final')

#backup_df = daily_backup.get_as_df()
#%%
#backup_df = pd.concat([backup_df,daily_req_df2],axis=0)

#%%

#daily_backup.set_dataframe(backup_df,start='B1',end='J')

# %%
