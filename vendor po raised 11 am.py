#%%
import pandas as pd
import numpy as np
import pygsheets
from datetime import datetime,timedelta

#%%
gc = pygsheets.authorize(service_file='C:/Users/Dell/Documents/Python Scripts/Note/test-api-403913-222895678b66.json')

#%%
# open daily order backup

sh = gc.open_by_key('19LOa1geQijLD9AiBPAOGOvxsyQ_Mat6JFymUthI24AU')

sh1 = sh.worksheet_by_title('Daily Order Backup')

bks_sh = sh1.get_as_df(start='A1',end='O')

#bks_sh.get_as_df(start='A1',end='O')
#%%
bks_sh['Date'] = pd.to_datetime(bks_sh['Date'],format = '%d-%m-%Y')
#%%
yesterday_date = datetime.now()-timedelta(days=1)

yesterday= yesterday_date.date()
#%%
yesterday_data = bks_sh[bks_sh['Date'].dt.date==yesterday]
#%%
#bks_sh2 = bks_sh[yesterday]

#%%
print(yesterday_data)

#%%

wks_bkp = sh.worksheet_by_title('Vendor Po backup')

wks_bkp_df = wks_bkp.get_as_df(start='A1',end='O')

wks_bkp_df2 = pd.concat([wks_bkp_df,yesterday_data],axis=0)

wks_bkp.set_dataframe(wks_bkp_df2,start='A1',end='O')




# %%
