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

shelf_sh = gc.open_by_key('1bqItcCLvRC_pHtB90oKcxJcuCMe87CbpliY01xsBXnE')

oil_sh = shelf_sh.worksheet_by_title('oil po')

oil_sh.set_dataframe(oil_df,start='A1')

# %% fillrate data query

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
fill_query = """select distinct
  facility_id
  , f.name
  , inward_date
  , o.vendor_id
  , vs.vendor_display_name
  , material_inwarding_item_id
  --, purchase_invoice_number
  --, document_id
  , o.order_item_id
  , ord.order_id as po_number
  , prod.jpin
  , prod.title
  , ord.buyer_id
  , orgname
  , o.mrp
  --, needs_qc
  , done_by
  , email as employee
  ,(ord.quantity) as ordered_quantity
  ,(ord.order_item_amount) as ordered_value
  --,(po.invoice_amount) as invoice_value
  ,(units_filled) as units_filled
  , (cast(units_filled as float) * ord.selling_price) as value_inwarded
from
  (
    select
      mi.inward_date
      , done_by
      , email
      , mrp
      --, needs_qc
      , facility_id
      , vendor_id
      , vendor_invoice_id
      , material_inwarding_item_id
      , split_part(processed_order_item_ids, ',', cast(n.number as int) + 1) as order_item_id
      , split_part(processed_order_item_quantities, ',', cast(n.number as int) + 1) as units_filled
      , orgname
    from
      (
        select
          facilityid as facility_id
          , vendorid as vendor_id
          , vendor_invoice_id
          , date(mi.src_created_time) as inward_date
          , material_inwarding_item_id
          , processed_order_item_ids
          , processed_order_item_quantities
          , org_id
          , mi.quantity
          , mi.mrp
          , mi.needs_qc
          , f.display_name as done_by
          , f.natural_id as email
          , org.org_name as orgname
        from
          material_inwarding_item_snapshot mi
          left join dw_bradman.inventory_item_movement_request i on
            i.inv_movement_ref_id = material_inwarding_item_id
          left join fc_user f on
            f.natural_id = i.last_updated_by
          left join org_profile_snapshot org on 
          org.org_profile_id = org_id
        where
          date(mi.src_created_time) = current_date
          and inwardingtype = 'PHYSICAL'
          and mi.status != 'FAILED'
      )
      mi
      join numbers n on
        n.number < regexp_count(processed_order_item_ids, ',')
  )
  o
/*left join purchase_invoice_snapshot po on
  o.vendor_invoice_id = po.purchase_invoice_id*/
left join dw_bradman.facility f on
  f.natural_id = facility_id
left join dw_address.city c on
  c.natural_id = f.city_id
left join vendor_snapshot vs on
  vs.vendor_id = o.vendor_id
left join bolt_order_item_v2_snapshot ord on
  ord.order_item_id = o.order_item_id
left join listing_snapshot ls on
  ls.listing_id = ord.listing_id
left join sellerproduct_snapshot sps on
  sps.sp_id = isnull(ls.sp_id, ls.channel_spid)
left join hevo_product prod on
  prod.jpin = sps.jpin
where
-- o.order_item_id='BLTORDITM-1276297540' and 
f.facility_type != 'RETAIL_STORE'
and inward_date = current_date
and f.name in (
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
        'Sutlej/Gomati',
        'Vajra',
        'Vayu',
        'Vikrant'
      )
/*group by
1
, 2
, 3
, 4
, 5
, 6
, 7
, 8
, 9
, 10
, 11
, 12
, 13
, 14
, 15
, 16
, 17*/
order by
2
, 4 asc"""

# %%

df_fillrate = pd.read_sql(fill_query,engine)

# %%
import pygsheets
import pandas as pd
import numpy as np

#authorization
gc = pygsheets.authorize(service_file='C:/Users/Dell/Documents/Python Scripts/Note/test-api-403913-222895678b66.json')

open_fill = gc.open('Shelf Life Request Sheet')

open_shelf = open_fill.worksheet_by_title('Fill Rate Sheet')

open_shelf.set_dataframe(df_fillrate,start='A1')
# %%
    
