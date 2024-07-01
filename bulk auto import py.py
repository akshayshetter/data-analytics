# %%
import pandas as pd
import numpy as np
import pygsheets

# %%
gc = pygsheets.authorize(service_file='C:/Users/Dell/Documents/Python Scripts/Note/test-api-403913-222895678b66.json')

# %%
# open bulk buying req sheet and Otb sheet

bulk_sh = gc.open_by_key('1abyXCkkwCNtIP7Bf95Mi8PZw3HWvFBQoKorTiYV7KV8') # bulk buying sheet

# get values from the bulk sheet

bulk_req = bulk_sh.worksheet_by_title('Bulk Request Sheet')

#%%
#%%
bulk_data = bulk_req.get_as_df()

bulk_data_2 = bulk_data[bulk_data['Jpin']!=""]

bulk_data_3 = bulk_data_2[((bulk_data_2['City/Central team Approval'] == 'Approved') | (bulk_data_2['Status'] == 'Approved')) & (bulk_data_2['Jpin'] != '') & (pd.to_numeric(bulk_data_2['New Requested Otb Limit'], errors='coerce') > pd.to_numeric(bulk_data_2['Current Otb Limit'], errors='coerce'))]

req_data = ['Date','City Id','CITY', 'Jpin','New Qty in units','Cost Price per unit','Po Lead time','Title','Current Inventory in units(Incl Open Po)','New Inventory','Super Category','Current Otb Limit',	'New Requested Otb Limit']

bulk_data_sh = bulk_data_3[req_data]

# check for duplicate data

bulk_data_sh2 = bulk_data_sh.drop_duplicates(subset=['CITY','Jpin'])

# %%
print(bulk_data_sh2)
# %%
# Transfering approved data Otb sheet

otb_sh = gc.open_by_key('1o-l47SqrZei2ZRoij99-z3079SUz6oMrM0I0y-zRAHA') # OTB sheeet

bulk_otb = otb_sh.worksheet_by_title('Bulk buy Otb')

bulk_otb.set_dataframe(bulk_data_sh2,start='B1')
# %%
import numpy as np
import pandas as pd
import pygsheets
from datetime import datetime, timedelta

#%%
#otb_sh = gc.open_by_key('1o-l47SqrZei2ZRoij99-z3079SUz6oMrM0I0y-zRAHA') 

#bulk_otb = otb_sh.worksheet_by_title('Bulk buy Otb')

#bulk_otb.clear()

#bulk_otb_df = bulk_otb.get_as_df(start='A2',end='L')

#bulk_otb_df.clear()

#bulk_otb_df['Date'] = pd.to_datetime(bulk_otb_df['Date'], format='%d-%m-%Y')

#four_days_back = datetime.now()-timedelta(days=4)

#fourdays = four_days_back.date()

#filtered_df = bulk_otb_df(bulk_otb_df['Date'].dt.date == fourdays)
#%%


# %%
