# %%
import pygsheets
import numpy as np
import pandas as pd

gc = pygsheets.authorize(service_file='C:/Users/Dell/Documents/Python Scripts/Note/test-api-403913-222895678b66.json')

np_sheet = gc.open('New Planning Working BizOps')

wacp_sh = np_sheet.worksheet_by_title('WACP')

wacp_df = wacp_sh.get_as_df(start='A',end='T')

wacp_df.reset_index(drop=True,inplace=True)

wacp_df = wacp_df[wacp_df['category_name']=="Oil, Ghee and Vanaspati"]


#%%
import pygsheets
import numpy as np
import pandas as pd

gc = pygsheets.authorize(service_file='C:/Users/Dell/Documents/Python Scripts/Note/test-api-403913-222895678b66.json')
#%%

sh = gc.open('PO Request sheet OGV')

po_wks = sh.worksheet_by_title('WACP')

po_wks.set_dataframe(wacp_df,start='B1')

#%%