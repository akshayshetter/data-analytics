# %%
import pygsheets
import pandas as pd
import numpy as np
from datetime import datetime,timedelta

gc = pygsheets.authorize(service_file='C:/Users/Dell/Documents/Python Scripts/Note/test-api-403913-222895678b66.json')

# %%
from urllib.parse import quote
import requests
import json
import psycopg2
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

gc = pygsheets.authorize(service_file='C:/Users/Dell/Documents/Python Scripts/Note/test-api-403913-222895678b66.json')

engine = create_engine('postgresql+psycopg2://aashutosh_aditya:%s@datawarehouse-cluster.cgatqp75ezrh.ap-southeast-1.redshift.amazonaws.com:5439/datawarehousedb'%quote('t3qTTa9DpuI5'),echo=True)
engine.connect()

# %%

oil_query = """select distinct boi.order_item_id,boi.order_item_status,boi.order_id,boi.buyer_id as customer_id,ops.org_name as CustomerName,ps.jpin,ps.title as productTitle,boi.listing_id,ls.mrp,ls.selling_price,
boi.order_item_amount/boi.quantity as FinalPricePerUnit,boi.quantity as orderedQuantity,boi.created_units,boi.underprocessunitquantity,boi.ready_to_ship_units,boi.delivered_units,boi.cancelled_units,boi.seller_id,op.org_name as sellerName,boi.order_item_amount,boi.src_created_time as orderplacedtimestamp,boi.last_updated_by,boi.status_update_history,r.facilityid,date(pr.updated_promise_time) as promise_date,f.name as facility_name,cat.category_name as category_name
from bolt_order_item_v2_snapshot boi
left join hevo_replenishmentpurchaseordermetadata r on r.orderitemid=boi.order_item_id
left join org_profile_snapshot ops on ops.org_profile_id=boi.buyer_id
left join org_profile_snapshot op on op.org_profile_id=boi.seller_id
left join listing_snapshot ls on ls.listing_id=boi.listing_id
left join sellerproduct_snapshot sp on sp.sp_id=ls.channel_spid
left join hevo_product ps on ps.jpin=sp.jpin
left join promise_snapshot pr on pr.promised_entity_id = boi.order_item_id
left join dw_bradman.facility f on f.natural_id=r.facilityid
left join category cat on cat.pvid = ps.pvid
where boltordertype='REPLENISH'
and f.facility_type!='RETAIL_STORE'
and order_item_status NOT in ('CANCELLED','Cancelled')
and boi.quantity >0
and boi.order_item_status= 'Under Process'
and boi.src_created_time>=current_date
and date(pr.updated_promise_time)=current_date
and cat.category_name in ( 'Oil, Ghee and Vanaspati')
and f.name in('FC-Ahmedabad','FC-Bhubaneswar','FC-Chandigarh','FC-Faridabad','FC-Ghaziabad','FC-Gurgaon','FC-Jaipur','FC-Lucknow','FC-Ludhiana','FC-Patna','FC-Pune','FC-Ranchi','Arihant','BLR_HNU','BLR_KDL','BLR_KML','BLR_KSL','BLR_PNY','FC-Hyderabad','HYD_ATP','HYD_NCH','LKO_KNP','PUN_TLW','Sutlej/Gomati','Vayu','Vikrant')
order by 25 desc"""

oil_df = pd.read_sql(oil_query,engine)
 # %%
# load data to sheet

shelf_sh = gc.open_by_key('19LOa1geQijLD9AiBPAOGOvxsyQ_Mat6JFymUthI24AU')

oil_sh = shelf_sh.worksheet_by_title('Po raised')

oil_sh.set_dataframe(oil_df,start='A1')
# %%
