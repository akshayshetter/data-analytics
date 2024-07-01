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

engine = create_engine('postgresql+psycopg2://aashutosh_aditya:%s@datawarehouse-cluster.cgatqp75ezrh.ap-southeast-1.redshift.amazonaws.com:5439/datawarehousedb'%quote('t3qTTa9DpuI5'),echo=True)
engine.connect()

# %%
query = """select * from 
(
  select distinct category_name,
ps.jpin,ps.title,
-- listingid, 
MarginCalculationMethod,
lpdi.geoid,
  case when geoname in ('Patiala') then 'Chandigarh'
  when geoname in ('Hubli') then 'Hubballi'
  when geoname in ('Bengaluru') then 'Banglore'
  when geoname in ('Khagaul') then 'Patna'
else geoname end as 
  geoname,
margintargetreftype,
targetmargin,
marginconfigurationauditid,
 (TIMESTAMP 'epoch' + lpdi.createdtime / 1000*INTERVAL '1 second' +INTERVAL '5:30')  as createdtime_,
(TIMESTAMP 'epoch' + lpdi.lastupdatedtime / 1000*INTERVAL '1 second' +INTERVAL '5:30')  as lastupdatedtime_,
  originevententityid,originevententitytype,
JSON_EXTRACT_PATH_TEXT(listinggeoinventoryaccountinginfo, 'accountedInventoryCount') as accountedInventoryCount,
JSON_EXTRACT_PATH_TEXT(listinggeoinventoryaccountinginfo, 'unaccountedInventoryCount') as unaccountedInventoryCount,
JSON_EXTRACT_PATH_TEXT(listinggeoinventoryaccountinginfo, 'wacpForAccountedInventory') as wacpForAccountedInventory,
JSON_EXTRACT_PATH_TEXT(listinggeoinventoryaccountinginfo, 'wacpForUnaccountedInventory') as wacpForUnaccountedInventory,
JSON_EXTRACT_PATH_TEXT(listinggeoinventoryaccountinginfo, 'calculatedWACP') 
as calculatedWACP,
 basesellingprice,
ROW_NUMBER() over ( partition by ps.jpin, lpdi.geoid order by lpdi.createdtime desc) as rank2
from hevo_listing_pricing_decision_info lpdi
left join listing_sales_attributes_snapshot_ ls on ls.listing_id=lpdi.listingid
  and ls.geo_id=lpdi.geoid
left join sellerproduct_snapshot sp on sp.sp_id=ls.sp_id
left join hevo_product ps on ps.jpin=sp.jpin
left join category cat on ps.pvid = cat.pvid 
left join hevo_geography h on h.geoid=lpdi.geoid
 where 
--ps.jpin='JPIN-1304475858'
-- and geoname='Bengaluru'
-- listingid='LST-1209548643' and
 -- listinggeoinventoryaccountinginfo is not null
 distributed='f'
  and category_name in ('Oil, Ghee and Vanaspati')  
)
 where rank2=1
"""

#cursor.execute(query)

# %%
df = pd.read_sql(query,engine)
print(df)
# %%
sh = gc.open('PO Request sheet OGV')

po_wks = sh.worksheet_by_title('WACP')

po_wks.set_dataframe(df,start='B1')
# %%
