#%%
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


engine = create_engine('postgresql+psycopg2://aashutosh_aditya:%s@datawarehouse-cluster.cgatqp75ezrh.ap-southeast-1.redshift.amazonaws.com:5439/datawarehousedb'%quote('t3qTTa9DpuI5'),echo=True)
engine.connect()

#%%
query = """SELECT
    jpin,
    title,
    f.name,
    c.category_name,
    SUM(ii.left_qty)
FROM
    dw_bradman.inventory_item ii
LEFT JOIN
    dw_bradman.space s ON s.id = ii.space_id
LEFT JOIN
    dw_bradman.facility f ON f.id = s.facility_id
LEFT JOIN
    product_snapshot_ p ON p.jpin = ii.product_id
LEFT JOIN
    category c ON c.pvid = p.pvid
WHERE
    c.category_name IN ('Sugar and Jaggery')
    AND f.name IN ('Arihant', 'BLR_HNU', 'BLR_KDL', 'BLR_KML', 'BLR_KSL', 'BLR_MRT', 'BLR_PNY', 'FC-Ahmedabad', 'FC-Bhubaneswar', 'FC-Hyderabad', 'FC-Lucknow', 'FC-Patna', 'FC-Pune', 'FC-Ranchi', 'HYD_ATP', 'HYD_BAL', 'HYD_NCH', 'HYD_SAN')
    AND ii.state = 'SELLABLE'
    AND ii.status = 'ACTIVE'
GROUP BY
    1, 2, 3, 4;
"""

#%%
df = pd.read_sql(query,engine)

# %% Statples Data

sheet = gc.open('Sugar Instock') # Inwarding data sheet

wks_1 = sheet.worksheet_by_title('Instock')

wks_1.clear(start ='A1',end='Z')

wks_1.set_dataframe(df,start='A1')
# %%
