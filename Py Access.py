# %%
import pygsheets
import pandas as pd
import numpy as np

#authorization
gc = pygsheets.authorize(service_file='C:/Users/Dell/Downloads/test-api-403913-222895678b66.json')


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



# %%
#import psycopg2

#conn = psycopg2.connect(
#    host = 'datawarehouse-cluster.cgatqp75ezrh.ap-southeast-1.redshift.amazonaws.com',
#    dbname = 'datawarehousedb',
#    port = '5439',
#    user = 'aashutosh_aditya',
#    password = 't3qTTa9DpuI5'
#)

#cursor = conn.cursor()

# %%

query = """select *
from cp_table 
where ts= ( select max(ts) from cp_table)
"""

#cursor.execute(query)

# %%

df = pd.read_sql(query,engine)
print(df)
# %%

sh = gc.open('Test Api Sheet')

wks = sh.worksheet_by_title('Sheet1')

wks.clear(start='A',end='Z')

wks.set_dataframe(df,start='A1')
