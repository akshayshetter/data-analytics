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

query = """select distinct o.city_name,
o.city_id,
o.jpin,o.title,
o.facility_NAME, 
space_allocated as opening_space_allocated,
o.inv as inv,
o.SELLABLE_inv as sellable_inv

from (
select distinct
isnull(z.city_name,o.city_name) as city_name,
isnull(z.city_id,o.city_id) as city_id,  
isnull(z.entity_id,o.product_id) as jpin,
isnull(z.title,o.title) as title,
isnull(o.facility,'NO ALLOCATED SPACE') as facility, 
isnull(name,'NO ALLOCATED SPACE') as facility_NAME,
isnull(o.space_allocated,0) as space_allocated,
inv,
  SELLABLE_inv
  from 
(
    select distinct entity_id,bzgrp_id,bzidgrpname,be.exclusive,bg.city_id,
  case when h.geoname in ('Khagaul') then 'Patna'
           when h.geoname in ('Patiala') then 'Chandigarh'
           WHEN h.geoname in ('Faridabad') then 'Gurgaon' 
           when h.geoname in ('Bengaluru') then 'Bangalore'
           when h.geoname in ('Hubli') then 'Hubballi'
           when h.geoname in ('Mysuru') then 'Mysore'
           else h.geoname end as city_name,
           ps.title
from business_group_entity_exclusivity_snapshot be
left join business_group_snapshot_ bs on bs.bzidgrpid=be.bzgrp_id
 join bg__city_mapping bg on bg.businessgroupid=be.bzgrp_id     
 left join dw_address.city c on c.natural_id = bg.city_id
  left join hevo_geography h on h.geoentityid = c.natural_id
   left join hevo_product ps on ps.jpin=entity_id
 where  bs.status='ACTIVE'
-- and entity_id='JPIN-1304573479'
and bs.grouppurpose='PRODUCT_TARGETING'
-- and entity_id='JPIN-1304477048'

) z 

full join  (
        select distinct case when h.geoname in ('Khagaul') then 'Patna'
           when h.geoname in ('Patiala') then 'Chandigarh'
           WHEN h.geoname in ('Faridabad') then 'Gurgaon' 
           when h.geoname in ('Bengaluru') then 'Bangalore'
           when h.geoname in ('Hubli') then 'Hubballi'
           when h.geoname in ('Mysuru') then 'Mysore'
           else h.geoname end as city_name,
   c.natural_id as city_id,
   f.natural_id as facility,f.name,
            spqm.product_id
 ,ps.title,
            sum( max_allowed_qty) as space_allocated
        from
            dw_bradman.space_product_qty_map spqm
   left join product_snapshot_ ps on ps.jpin=spqm.product_id
            left join dw_bradman.facility f on spqm.facility_id = f.id
            left join dw_address.city c on c.natural_id = f.city_id
            LEFT JOIN hevo_geography h on h.geoentityid = c.natural_id
        where
            spqm.association_status = 'ACTIVE'
            and spqm.purpose = 'STORAGE'
   and f.facility_type!='RETAIL_STORE'
--  and spqm.product_id='JPIN-1304477048'
 -- and spqm.product_id='JPIN-1304573479'
   group by 1,2,3,4,5,6
    )  o 
on 
o.product_id=z.entity_id
and o.city_id=z.city_id
left join (
   select distinct f.natural_id as facility,
            ii.product_id,
            sum( left_qty) as inv, 
  sum( case when ii.state in( 'SELLABLE') 
        and ii.status in( 'ACTIVE','ONHOLD') then LEFT_QTY end ) as SELLABLE_inv
  from dw_bradman.inventory_item ii
  left join dw_bradman.space s on s.id=ii.space_id
  left join dw_bradman.facility f on f.id=s.facility_id
  left join dw_address.city c on c.natural_id=f.city_id
  left join product_snapshot_ ps on ps.jpin=ii.product_id
  where  ii.state in( 'SELLABLE' ,'INWARDED','UNDER_TRANSFER')
  and ii.status in( 'ACTIVE','ONHOLD') 
     and f.facility_type!='RETAIL_STORE'
--  and ii.product_id='JPIN-1304477048'
  group by 1,2
  ) a on a.facility=o.facility
and a.product_id=o.product_id
) o
 --where o.jpin in ( 'JPIN-1304473942',	'JPIN-1304465820'	,'JPIN-1304465896')
 where o.space_allocated > 0
 --and o.jpin = 'JPIN-1304470724'
 and facility_name in (
            'Arihant',
            'BLR_HNU',
            'BLR_KDL',
            'BLR_KML',
            'BLR_KSL',
            'BLR_MRT',
            'BLR_PNY',
            'CHE_HUB',
            'FBD_HUB',
            'FC-Ahmedabad',
            'FC-Bhubaneswar',
            'FC-Chandigarh',
            'FC-Chennai',
            'FC-Coimbatore',
            'FC-Ghaziabad',
            'FC-Hyderabad',
            'FC-Jaipur',
            'FC-Lucknow',
            'FC-Ludhiana',
            'FC-Patna',
            'FC-Pune',
            'FC-Ranchi',
            'FC-Trichy',
            'FC-Vijayawada',
            'GGN_BDR',
            'HYD_ATP',
            'HYD_BAL',
            'HYD_NCH',
            'JH_JSD_BLP',
            'LKO_KNP',
            'MYS_AGR',
            'PUN_PSL',
            'SALEM_HUB',
            'Sutlej/Gomati',
            'Vajra',
            'Vayu',
            'Vikrant'
          )
group by 1,2,3,4,5,6,7,8
"""

#cursor.execute(query)

# %%

df = pd.read_sql(query,engine)
print(df)
# %%

sh = gc.open('OTB Request Sheet')

wks = sh.worksheet_by_title('Inventory data')

wks.clear(start='A',end='H')

wks.set_dataframe(df,start='A1')


# %%
# %%
# import pandas as pd

# import pygsheets

# gc = pygsheets.authorize(service_file='/Users/Dell/Downloads/test-api-403913-222895678b66.json')

#%%
# sheet=gc.open('OGV Hub based DRR Pilot')

#%%
# wks = sheet.worksheet_by_title('Clean Data')

# print(wks)

#%%
# dataframe = wks.get_as_df(index_column=1)

# print(dataframe)
# %%
# import pandas as pd

# %%
# sh = gc.open('OTB Request Sheet')

# wks2 = sh.worksheet_by_title('DRR')

# wks2.clear(start='A',end='F')

# wks2.set_dataframe(dataframe,start='A1')

# %%
import pandas as pd

import pygsheets

gc = pygsheets.authorize(service_file='/Users/Dell/Downloads/test-api-403913-222895678b66.json')

shn = gc.open('JPIN OTB')

#%%

wksn = shn.worksheet_by_title('OTB Limits')

#%%
print(wksn)

# %%
#%%
import pandas as pd

import pygsheets

gc = pygsheets.authorize(service_file='/Users/Dell/Downloads/test-api-403913-222895678b66.json')

shn = gc.open('JPIN OTB')

#%%

wksn = shn.worksheet_by_title('OTB Limits')

#%%
print(wksn)

# %%
df = wksn.get_as_df(start='A',end='W')

print(df)
# %%

import pandas as pd

# %%
sh = gc.open('OTB Request Sheet')

wks3 = sh.worksheet_by_title('OTB')

wks3.set_dataframe(df,start='A1')
# %%
