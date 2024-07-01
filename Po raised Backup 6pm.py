#%%
import pandas as pd
import numpy as np
import pygsheets

gc = pygsheets.authorize(service_file='C:/Users/Dell/Documents/Python Scripts/Note/test-api-403913-222895678b66.json')

#%%
# opens order sheet 
sh = gc.open_by_key('19LOa1geQijLD9AiBPAOGOvxsyQ_Mat6JFymUthI24AU')

order = sh.worksheet_by_title('Order Qty')

order_df = order.get_as_df(start='A1',end='M')
#%%
print(order_df)
# %%
# open backup sheet
bk_sh = gc.open_by_key('19LOa1geQijLD9AiBPAOGOvxsyQ_Mat6JFymUthI24AU')
#%%

bkp = bk_sh.worksheet_by_title('Daily Order Backup')
#%%
bkp2 = bkp.get_as_df(start='A1',end='M')
#%%

bkp_df2 = pd.concat([bkp2,order_df])

#%%
bkp.set_dataframe(bkp_df2,start='A1')
# %%
