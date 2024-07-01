# %%
import numpy as np
import pandas as pd
import pygsheets
from datetime import datetime, timedelta
gc = pygsheets.authorize(service_file='C:/Users/Dell/Documents/Python Scripts/Note/test-api-403913-222895678b66.json')

# %%
# opnen bulk request sheet 
bulk_req = gc.open_by_key('1abyXCkkwCNtIP7Bf95Mi8PZw3HWvFBQoKorTiYV7KV8') # bull request sheet

bulk_bkp = bulk_req.worksheet_by_title('Bulk Request Sheet')

bulk_df = bulk_bkp.get_as_df()

bulk_df['Date'] = pd.to_datetime(bulk_df['Date'], format = '%d-%m-%Y')

bulk_df['formated_date'] = bulk_df['Date'].dt.strftime('%Y-%m-%d')

bulk_df['formated_date'] = pd.to_datetime(bulk_df['formated_date']).dt.strftime('%d-%m-%Y')

today = datetime.now()

today_date = today.date()

filtered_df = bulk_df[(bulk_df['Date'].dt.date == today_date) & (bulk_df['Jpin'] !='')]

#%%
print(filtered_df)

# %%
# open backup sheet
bulk_req1 = gc.open_by_key('1abyXCkkwCNtIP7Bf95Mi8PZw3HWvFBQoKorTiYV7KV8') 

bulk_bkp_df = bulk_req1.worksheet_by_title('Bulk backup')

bulk_req2 = bulk_bkp_df.get_as_df()

bulk_bkp_1 = pd.concat([bulk_req2,filtered_df],axis=0)

bulk_bkp_df.set_dataframe(bulk_bkp_1,start='A1')
#%%
# getting 4 days back data 
#fourdays_date = datetime.now()-timedelta(days=4)

#fourdays_back_date = fourdays_date.date()

# %%
bulk_req = gc.open_by_key('1abyXCkkwCNtIP7Bf95Mi8PZw3HWvFBQoKorTiYV7KV8') # bull request sheet
bulk_bkp = bulk_req.worksheet_by_title('Bulk Request Sheet')
bulk_df=bulk_bkp.get_as_df(start='B',end='E')
bulk_df
#%%
bulk_df['Date'] = pd.to_datetime(bulk_df['Date'],format = '%d-%m-%Y')
bulk_df
#%%
from datetime import datetime, timedelta
# get current date
fourday_back_date = datetime.now() - timedelta(days=4)
fourday_back_date
# %%
dft2=bulk_df[bulk_df['Date']>fourday_back_date]
#%%
dft2
#%%
sh3 = gc.open ('Bulk Buying Request Sheet')
working_sheet1 = sh3.worksheet_by_title('Bulk Request Sheet')
#creating copy of dft2 to avoid slicing
dft2_copy = dft2.copy()
#%%
working_sheet1.clear(start='B',end='E')
#working_sheet1.rows = dft4.shape[0]
working_sheet1.set_dataframe(dft2_copy,start = 'B1')

#%%
wks=bulk_req1.worksheet_by_title('Bulk Request Sheet')
dft3=wks.get_as_df(start='H',end='K')
dft3
#%%
dft4=dft3.iloc[dft2.index].copy()
#%%
working_sheet1 = sh3.worksheet_by_title('Bulk Request Sheet')
working_sheet1.clear(start='H',end='K')
#working_sheet1.rows = dft4.shape[0]
working_sheet1.set_dataframe(dft4,start = 'H1')

# %%
otb_sh = gc.open_by_key('1o-l47SqrZei2ZRoij99-z3079SUz6oMrM0I0y-zRAHA') 

bulk_otb = otb_sh.worksheet_by_title('Bulk buy Otb')

bulk_otb.clear(start='B2',end='N')
# %%
