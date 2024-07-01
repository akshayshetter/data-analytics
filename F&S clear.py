#%%
import pandas as pd
import numpy as np
import pygsheets

gc = pygsheets.authorize(service_file='C:/Users/Dell/Documents/Python Scripts/Note/test-api-403913-222895678b66.json')
#%%
shB1 = gc.open_by_key('1AMfg6AQ4SA18Nx4cALPl6sIM2qrDpghgovqSJks2TB0') #Bangalore sheet
sh1 = shB1.worksheet_by_title('Flours & Sooji')
df1 = sh1.get_as_df()
# %%
print(df1)
# %%
sh1.clear(start='R2',end='R1000')
# %%
shA1 = gc.open_by_key('1uIc-zB1Nou3Ewcc9Xz_EdcQB2UG6g_D7nCHiQTODx18') #Ahmedabad sheet
sh2 = shA1.worksheet_by_title('Flours & Sooji')
sh2.clear(start='R2',end='R1000')
#%%
shP1 = gc.open_by_key('1vuxsfgaG-UIbA6OIOq8ZTNB-w1JKDfDGH-PeIYGm2yI') #Pune
sh3 = shP1.worksheet_by_title('Flours & Sooji')
sh3.clear(start='R2',end='R1000')
#%%
shH1 = gc.open_by_key('1ayabFhnVjRvVa_icOIY42B5XM6ktAqMG05IpkG_VCdI') #Hyderabad
sh4 = shH1.worksheet_by_title('Flours and Sooji')
sh4.clear(start='R2',end='R1000')
#%%
shB2 = gc.open_by_key('1Fd3jKBPkhHacX4E8F-z7ZX2xXudDkS1upNBLd00a3x0') #Bhubaneshwar
sh5 = shB2.worksheet_by_title('Flours and Sooji')
sh5.clear(start='R2',end='R1000')
#%%
shR1 = gc.open_by_key('1FWEFJ18kfYWSsPL5UMutJ5sk6jYSIHwvtty3CRnjZ3E') #Ranchi
sh6 = shR1.worksheet_by_title('Flours and Sooji')
sh6.clear(start='R2',end='R1000')
#%%
shP1 = gc.open_by_key('1GTRO3MJ3qFAKOs_veKEz8hjW8rQzlsjiihCFj4pmGx0') #Patna
sh7 = shP1.worksheet_by_title('Flours and Sooji')
sh7.clear(start='R2',end='R1000')
#%%
shL1 = gc.open_by_key('1V9iq9WGeLNvtUt1ewA7xxYPJ4_JsXKUuSGYr1_SBwck') #Lucknow
sh8 = shL1.worksheet_by_title('Flours and Sooji')
sh8.clear(start='R2',end='R1000')
# %%
