# %%
import pandas as pd
import numpy as np
import pygsheets
from datetime import datetime,timedelta

gc = pygsheets.authorize(service_file='C:/Users/Dell/Documents/Python Scripts/Note/test-api-403913-222895678b66.json')


#%%
#open backup file

sh = gc.open_by_key('1_c2QsdGeJbJEvMO4kRAjDvWxfuZqkdE4LnLM1jF9Rfw')

wks_sh = sh.worksheet_by_title('Backup final')

work_df = wks_sh.get_as_df(start='A1',end='M')

#%%

wks2_sh = sh.worksheet_by_title('Final Summary')

wks2_df = wks2_sh.get_as_df(start='A1',end='M')

wk2_df = pd.concat([wks2_df,work_df],axis=0)

wks2_sh.set_dataframe(wk2_df,start='A1',end='M')

#%%