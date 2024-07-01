# %%
import pandas as pd
import numpy as np
import pygsheets
from datetime import datetime,timedelta

gc = pygsheets.authorize(service_file = 'C:/Users/Dell/Documents/Python Scripts/Note/test-api-403913-222895678b66.json')

#%%

sh = gc. open_by_key('1_c2QsdGeJbJEvMO4kRAjDvWxfuZqkdE4LnLM1jF9Rfw')

sh2 = sh.worksheet_by_title('Backup')

wks_sh = sh2.get_as_df()

wks_sh['Date'] = pd.to_datetime(wks_sh['Date'], format= '%d-%m-%Y')

yesterday_date = datetime.now()

yesterday = yesterday_date.date()

yesterday_data = wks_sh[wks_sh['Date'].dt.date == yesterday]

req_columns = ['City',	'JPIN',	'Title',	'Current Inventory',	'DRR',	'days of cover',	'Inventory Norm',	'System Suggested Cases',	'City-CITY Id-JPIN']

wks_sh2 = yesterday_data[req_columns]

wks_sh = yesterday_data

#%%
print(wks_sh2)

#%%
print(wks_sh)
# %%
sh2 = gc. open_by_key('1_c2QsdGeJbJEvMO4kRAjDvWxfuZqkdE4LnLM1jF9Rfw')

sh3 = sh2.worksheet_by_title('Backup final')

sh3.set_dataframe(wks_sh2,start='B1',end='J')
# %%

sh4 = gc.open_by_key('19LOa1geQijLD9AiBPAOGOvxsyQ_Mat6JFymUthI24AU')

wks_sh4 = sh4.worksheet_by_title('Cases to be ordered')

wks_sh4.set_dataframe(wks_sh,start='A1')

# %%
