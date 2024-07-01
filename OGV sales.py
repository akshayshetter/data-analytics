# %%
import pandas as pd
import numpy as np
import pygsheets

gc = pygsheets.authorize(service_file='C:/Users/Dell/Documents/Python Scripts/Note/test-api-403913-222895678b66.json')

# %%
from urllib.parse import quote
import requests
import json
import psycopg2
import pandas as pd
import numpy as np
from datetime import date
from sqlalchemy import create_engine

gc = pygsheets.authorize(service_file='C:/Users/Dell/Documents/Python Scripts/Note/test-api-403913-222895678b66.json')

engine = create_engine('postgresql+psycopg2://aashutosh_aditya:%s@datawarehouse-cluster.cgatqp75ezrh.ap-southeast-1.redshift.amazonaws.com:5439/datawarehousedb'%quote('t3qTTa9DpuI5'),echo=True)
engine.connect()

# %%
ogv_query = """select ad.addresscity as city,p.jpin,p.title, 
date(ord.src_created_time),
sum(
    (ord.quantity-ord.cancelled_units-ord.returned_units-ord.return_requested_quantity)
  )
from bolt_order_item_snapshot ord
    join customer_snapshot_ c on c.customerid = ord.buyer_id
    left join business_snapshot bs on bs.businessid=c.businessid
    left join listing_snapshot l on l.listing_id=ord.listing_id
    left join sellerproduct_snapshot sp on sp.sp_id=l.sp_id
    left join product_snapshot_ p on p.jpin=sp.jpin
    left join category cat on p.pvid = cat.pvid     
    left join brand_snapshot_ b on b.brandid=p.brandid 
    left join (select distinct(pvid),pvname from productvertical_snapshot) pv on pv.pvid=p.pvid join address_snapshot_ ad on ad.addressentityid = c.businessid and ad.addresstype = 'SHIPPING' and ad.addresscity in('Lucknow','Kanpur','Ahmedabad','Bhubaneswar','Chandigarh','Faridabad','Ghaziabad','Gurgaon','Jaipur','Lucknow','Ludhiana','Patna','Pune','Ranchi','Bengaluru',
'Tumkur','Mysore', 'Chikkabalapur','Hosur', 'Hassan' , 'Mysore','Hyderabad') 
where c.istestcustomer IS FALSE
AND   order_item_amount > 0
AND   ord.quantity > ord.cancelled_units + ord.returned_units + ord.return_requested_quantity
AND   (c.status = 'ACTIVE' OR c.status = 'ONHOLD')
--and date_part('hour',ord.src_created_time)<=date_part('hour',convert_timezone('GMT', 'NEWZONE -5:30', GETDATE()))-1 
and date(ord.src_created_time)>=current_date-8  and cat.category_name in ('Oil, Ghee and Vanaspati')
group by 1,2,3,4
order by ad.addresscity,p.jpin,p.title"""

ogv_df = pd.read_sql(ogv_query,engine)

date = pd.unique('date')

ogv_df = ogv_df[date]

ogv_df_p = ogv_df.pivot(columns=['city','jpin','title',pd.unique('date')], values='sum')

#%%
print(ogv_df_p)
# %%
ogv_data = gc.open_by_key('1_c2QsdGeJbJEvMO4kRAjDvWxfuZqkdE4LnLM1jF9Rfw')

ogv_sales = ogv_data.worksheet_by_title('Salesdata2')

ogv_sales.set_dataframe(ogv_df_p,start='B1')


# %%
