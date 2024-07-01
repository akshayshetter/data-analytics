#%%
import pandas as pd
import numpy as np
import pygsheets
from datetime import datetime,timedelta
import matplotlib.pyplot as plt

gc = pygsheets.authorize(service_file='C:/Users/Dell/Documents/Python Scripts/Note/test-api-403913-222895678b66.json')

#%%
# open shelf life backup sheet
sh1 = gc.open_by_key('1bqItcCLvRC_pHtB90oKcxJcuCMe87CbpliY01xsBXnE')
shbk = sh1.worksheet_by_title('Shelf life Backup')
shb_df = shbk.get_as_df(start='A1',end='Al')
#%%

shb_df['Date'] = pd.to_datetime(shb_df['Date'],format = '%d-%m-%Y')

#%%
last_monday = datetime.now()-timedelta(days=10) 

#%%
week_data = shb_df[shb_df['Date'] >=last_monday] # filters last 10 days data
#%%
filtered_data = week_data
#%%
print(filtered_data)
#%%
sh2 = sh1.worksheet_by_title('Backup Summary') # open backup summary sheet
#%%
sh2.clear(start='A1',end='AL')
#%%
sh2.set_dataframe(filtered_data,start='A1',end='AL')
# %%
# open summary sheet

sh3= sh1.worksheet_by_title('Summary')

summary = sh3.get_as_df(start='A1',end='O')

req_columns = ['FC Name','Vendor Name',	'Approved',	'Rejected','KAM Approval Needed','Rejected but Inwarded','Total Count']

summary_df = summary[req_columns]

sort_summary = summary_df.sort_values(by='Total Count', ascending=False)

sort_summary_df = sort_summary.head(10)
#%%
print(sort_summary_df)
# %%
#def conditional_formatting(row):
#%%
sort_summary_df.reset_index(drop=True, inplace=True)
sort_summary_df.index +=1
#%%
def color_gradient(val,min_val,max_val,cmap='Reds'):
    normed_val = (val-min_val)/(max_val-min_val)
    rgba_color = plt.cm.get_cmap(cmap)(normed_val)
    hex_color = "#{:02x}{:02x}{:02x}".format( int(rgba_color[0] * 255),
        int(rgba_color[1] * 255),
        int(rgba_color[2] * 255),
        int(rgba_color[3] * 255)
    )
    return f'background-color: {hex_color}'
    

min_val = sort_summary_df['Total Count'].min()
max_val = sort_summary_df['Total Count'].max()

styled_df = sort_summary_df.style.apply(lambda x: x.map(lambda val: color_gradient(val, min_val, max_val)),
                           subset=['Total Count'])

#%%
styled_df1 = styled_df.set_properties(**{'text-align':'center'})
#%%

styled_df2 = styled_df1.set_table_styles([
    {'selector': 'thead th', 'props': [('text-align', 'center')]},  # Align headers at the center
    {'selector': 'td', 'props': [('text-align', 'center'), ('border', '1px solid black')]},  # Align cell values at the center and add a border
])

#%%

#%%
import dataframe_image as dfi
dfi.export(styled_df,"C:/Users/Dell/Pictures/Saved Pictures/shelf_life_week_summary.png")
# %%
