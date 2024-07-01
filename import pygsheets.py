import pygsheets
import pandas as pd
#authorization
gc = pygsheets.authorize(service_file="/Users/jumbotail/Downloads/disco-arcana-350612-a783f897cd2d.json")
​
​
sh = gc.open ('Automation Test')
​
​
from urllib.parse import quote
import requests
import json
import psycopg2
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
​
​
​
engine = create_engine('postgresql+psycopg2://aashutosh_aditya:%s@datawarehouse-cluster.cgatqp75ezrh.ap-southeast-1.redshift.amazonaws.com:5439/datawarehousedb'%quote('t3qTTa9DpuI5'),echo=True)
engine.connect()
​
​
​
​
queryString_input= '''
​
select *
from cp_table 
where ts=( select max(ts) from cp_table)
​
​
'''
​
​
temp = pd.read_sql(queryString_input,engine)
​
temp=temp.fillna('')
​
wks = sh.worksheet_by_title('Sheet1')
​
wks.get_as_df()
​
wks.set_dataframe(temp,start = 'B1') 