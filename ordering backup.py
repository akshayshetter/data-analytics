# %%
import numpy as np
import pandas as pd
import pygsheets

gc = pygsheets.authorize(service_file='C:/Users/Dell/Documents/Python Scripts/Note/test-api-403913-222895678b66.json')

# %%
po_req = gc.open_by_key('19LOa1geQijLD9AiBPAOGOvxsyQ_Mat6JFymUthI24AU') # Po request sheet

po_req = po_req.worksheet_by_title(['AHMBD','Patna'])

po_req_backup = po_req.get_as_df()

# %%
print(po_req_backup)

# %%
