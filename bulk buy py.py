# %%
import pygsheets
import numpy as np
import pandas as pd

# authorisation
gc = pygsheets.authorize(service_file='C:/Users/Dell/Documents/Python Scripts/Note/test-api-403913-222895678b66.json')

#%%

#inventory data from shelf life sheet

shelf_sh = gc.open_by_key('1bqItcCLvRC_pHtB90oKcxJcuCMe87CbpliY01xsBXnE') # shelf life sheet

inventory_sh = shelf_sh.worksheet_by_title('Inventory Data')

inventory_df = inventory_sh.get_as_df()

#%%
print(inventory_df)

# %%

bulk_inv = gc.open_by_key('1abyXCkkwCNtIP7Bf95Mi8PZw3HWvFBQoKorTiYV7KV8') # bulk buying sheet

inv_sh = bulk_inv.worksheet_by_title('Inventory Data')

inv_sh.set_dataframe(inventory_df,start='A1')
# %%
# drr from jpin category sheet in shelf life sheet
jpin_drr = gc.open_by_key('1bqItcCLvRC_pHtB90oKcxJcuCMe87CbpliY01xsBXnE') # shelf life sheet

drr_sh = jpin_drr.worksheet_by_title('Jpin Category')

drr_sh_df = drr_sh.get_as_df()

main_col = ['CITY',	'CITY Id',	'JPIN',	'title',	'category_name',	'Super  - Category', 'case_size', 'cost_price',	'max_drr','Current Limit'] # getting required column

drr_df = drr_sh_df[main_col]
# %%
print(drr_df)
# %%
# transferring req drr data from above sheet
bulk_inv_drr = gc.open_by_key('1abyXCkkwCNtIP7Bf95Mi8PZw3HWvFBQoKorTiYV7KV8') # bulk byuing sheet

inv_sh_drr = bulk_inv_drr.worksheet_by_title('Jpin DRR')

inv_sh_drr.set_dataframe(drr_df,start='A1')

# %%
# Getting Max allocated space data
max_sh = gc.open_by_key('1e5QSuofcaHFqLm4-tTnAy5S4a9GLnUoIGJCN5QWeUZc') # daily bizops working sheet

main_sh = max_sh.worksheet_by_title('Main')

max_spc_df = main_sh.get_as_df(start='A',end='M')

#%%
print(max_spc_df)

#%%
bulk_inv_spc = gc.open_by_key('1abyXCkkwCNtIP7Bf95Mi8PZw3HWvFBQoKorTiYV7KV8') # bulk byuing sheet

inv_sh_spc = bulk_inv_spc.worksheet_by_title('Max Space')

inv_sh_spc.set_dataframe(max_spc_df,start='A1',end='M')

# %%
# current inventory value

inv_val = gc.open_by_key('1hJdSCthVaCJWYcXtOBwiMBTTGDU5BKIhPh9kVlZrLZo') # inventory slow moving sheet

main_val = inv_val.worksheet_by_title('Working')

val_inv_df = main_val.get_as_df()

#%%

req_val_col = ['Key',	'City',	'jpin',	'category_name',	'title',	'Total Stock Qty',	'Total Stock Amount']

value_df = val_inv_df[req_val_col].copy()

#%%
## print(value_df)

# %%
# getting current inv value data

val_inv = gc.open_by_key('1abyXCkkwCNtIP7Bf95Mi8PZw3HWvFBQoKorTiYV7KV8')

current_inv = val_inv.worksheet_by_title('inventory value')

current_inv.set_dataframe(value_df,start='A1')

# %%
