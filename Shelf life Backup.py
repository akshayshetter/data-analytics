# %%
import pygsheets
import pandas as pd
import numpy as np

#authorization
gc = pygsheets.authorize(service_file='C:/Users/Dell/Documents/Python Scripts/Note/test-api-403913-222895678b66.json')


# %%
from urllib.parse import quote
import requests
import json
import psycopg2
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# %% Create a back up sheet

shelf_life_sheet = gc.open_by_key('1bqItcCLvRC_pHtB90oKcxJcuCMe87CbpliY01xsBXnE')

shelf_req_sheet_wks = shelf_life_sheet.worksheet_by_title('Shelf Life Request Sheet')

shelf_df = shelf_req_sheet_wks.get_as_df()

shelf_df = shelf_df[shelf_df['Jpin']!='']


shelf_backup_wks = shelf_life_sheet.worksheet_by_title('Shelf life Backup')

shelf_backup_df = shelf_backup_wks.get_as_df()

shelf_backup_df = pd.concat([shelf_backup_df,shelf_df],axis=0)

shelf_backup_wks.set_dataframe(shelf_backup_df,start='A1')

# %%
import time
time.sleep(2)


shelf_req_sheet_wks.clear(start='C2',end='E1000')

shelf_req_sheet_wks.clear(start='G2',end='I1000')

# %%

