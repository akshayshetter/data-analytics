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
query = """select case when city.city_name in ('Hubballi','Belgaum','Dharwad','Davanagere','Haveri','Gadag') then 'Hubballi' 
when city.city_name in ('Bengaluru','Tumkur','Hosur','Bangalore','BENGALURU','BANGALORE','Chikkaballapura','Mysore','Mandya','Hassan') then 'Bangalore'
when city.city_name in ('Vijayawada','Guntur','GUNTUR') then 'Vijayawada'
when city.city_name in ('Tiruppur','Coimbatore','Erode','Mettupalayam','Salem',
'Pollachi') then 'Coimbatore'
when city.city_name in ('Chandigarh','Panchkula','Mohali','Ambala','Patiala','Sirhind','Kalka','Rupnagar','Kharar','Zirakpur','Dera Bassi','Pinjore','Chandi Mandir','Kurali','Nayagaon','Ropar','Balachaur','Rajpura') then 'Chandigarh'
when city.city_name in ('Vizianagaram','Vizag') THEN 'Vizag'
when city.city_name in ('Gurgaon','Faridabad') then 'Gurgaon'
when city.city_name in ('Lucknow','Kanpur') then 'Lucknow'
when city.city_name in ('Jaipur') then 'Jaipur'
when city.city_name in ('Ranchi') then 'Ranchi'
when city.city_name in ('Pune') then 'Pune'
when city.city_name in ('Hyderabad') then 'Hyderabad'
when city.city_name in ('Trichy') then 'Trichy'
when city.city_name in ('Chennai') then 'Chennai'
when city.city_name in ('Patna', 'Khagaul') then 'Patna'
when city.city_name in ('Ahmedabad') then 'Ahmedabad'
when city.city_name in ('Ludhiana') then 'Ludhiana'
when city.city_name in ('Jamshedpur') then 'Jamshedpur'
when city.city_name in ('Ghaziabad') then 'Ghaziabad'
when city.city_name in ('Bhubaneswar')then 'Bhubaneswar'
when city.city_name in ('Delhi','Mandoli') then 'Delhi'
ELSE 'ROI' END AS addresscity
,f.name as facility_name
, rpo.jpin
,ps.title as productTitle
, rpo.orderitemid
, vs.vendor_id
, vs.vendor_display_name
, sum(rpo.qtyraisingfor) as qty_raised_for
, sum(ord.order_item_amount) as po_value
--,sum(ord.delivered_units) as delivered_qty
, date(timestamp 'epoch' + (rpo.createdtime/1000) * interval '1 second' + interval '5 hours 30 minutes') as order_date
, date(timestamp 'epoch' + (rpo.promisetime/1000) * interval '1 second' + interval '5 hours 30 minutes') as initial_promise_date
, ord.order_id
, ms.manufacturername
,ord.cancelled_units as delivered_qty
,ord.order_item_status
,ord.last_updated_by
from hevo_replenishmentpurchaseordermetadata rpo
join bolt_order_item_v2_snapshot ord on rpo.orderitemid = ord.order_item_id
left join vendor_snapshot vs on vs.org_profile_id = ord.seller_id
left join hevo_product ps on ps.jpin = rpo.jpin
left join brand_snapshot_ bs on bs.brandid = ps.brandid
left join manufacturer_snapshot ms on ms.manufacturerid = bs.manufacturerid
left join dw_bradman.facility f on rpo.facilityid = f.natural_id
left join dw_address.city city on f.city_id = city.natural_id
left join category cat on cat.pvid = ps.pvid
where date(timestamp 'epoch' + (rpo.createdtime/1000) * interval '1 second' + interval '5 hours 30 minutes') >= current_date-2
--and date(timestamp 'epoch' + (rpo.promisetime/1000) * interval '1 second' + interval '5 hours 30 minutes') <= '2023-10-31'
--and rpo.createdtime >= (extract( epoch from current_date - 7)::bigint)*1000 and rpo.createdtime < (extract( epoch from current_date + 1)::bigint)*1000 and rpo.__hevo__marked_deleted = 'f'
--and ord.order_id in ('BLTORD-1347012082','BLTORD-1353553695')
and rpo.rporeasoncode != 'MISJIT'
--and ms.manufacturername in ('Unibic Foods India Private Limited')
and cat.category_name in ('Oil, Ghee and Vanaspati')
and NOT (
        (ord.order_item_status IN ('CANCELLED', 'Cancelled'))
        AND (ord.last_updated_by IN ('purchaseorder@aakepl.com', 'purchaseorder@bodega.co.in'))
    )
--and city.city_name in ('Bangalore')
group by 1,2,3,4,5,rpo.createdtime,vs.vendor_id,rpo.promisetime,ord.order_id,ms.manufacturername,ord.cancelled_units,vs.vendor_display_name,ord.order_item_status,ord.last_updated_by
"""
# %%
df = pd.read_sql(query,engine)
#%%
print(df)
#%%
new_columns = {'addresscity':'City','vendor_display_name':'Vendor_Name','vendor_id':'Vendor_ID','order_date':'Po_raised_date','qty_raised_for':'Po_Raised_Qty','delivered_qty':'Delivered_Qty','initial_promise_date':'Promise_Date','facility_name':'Facility_Name','jpin':'Jpin','producttitle':'Title'}
#%%
df.rename(columns=new_columns, inplace=True)
# %%
print(df)
# %%
req_columns = ['City','Facility_Name','Po_raised_date','Jpin','Title','Vendor_Name','Vendor_ID','Po_Raised_Qty', 'Delivered_Qty','Promise_Date' ]

df2 = df[req_columns]

#%%
print(df2)
# %%
from datetime import datetime,timedelta

yesterday_date = datetime.now()-timedelta(days=1)

yesterday = yesterday_date.date()

#%%
df2['Promise_Date'] = pd.to_datetime(df2['Promise_Date'])
#%%
yesterday_data = df2[df2['Promise_Date'].dt.date == yesterday]

req_columns = ['Po_raised_date','City','Facility_Name','Jpin','Title','Vendor_Name','Vendor_ID','Po_Raised_Qty', 'Delivered_Qty','Promise_Date']
#%%
yesterday_data2 = yesterday_data[req_columns]
#%%
print(yesterday_data2)
# %%
sheet = gc.open_by_key('19LOa1geQijLD9AiBPAOGOvxsyQ_Mat6JFymUthI24AU')

wks2 = sheet.worksheet_by_title('Po raised2')

wks2.clear(start='A1',end='J')

wks2.set_dataframe(yesterday_data2,start='A1',end='J')
# %%
