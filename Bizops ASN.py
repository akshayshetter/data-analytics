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
query = """select distinct
"FC-Name",
"PO promise date",
round(sum("Weight (Kg)")/1000) as "Planned Tonnage",
tonnage_capacity as "Agreed Tonnage",
round(sum("Total JPINs in PO")) as "Planned JPins",
jpin_capacity as "Agreed JPins",
round(sum("Total no of cases planned in PO")) as "Planned Cases"
from
(
    select distinct
    b.order_placed_timestamp_ as "ASN Timestamp",
    b.order_id as "PO Number",
    b.vendor_name as "Vendor Name",
    b.facility as "FC-Name",
    b.city_name as "City Name",
    date(promise_date) as "PO promise date",
    case when DATEPART(HOUR,promise_date) >= 0 and DATEPART(HOUR,promise_date) < 2 then '0-2'
    when DATEPART(HOUR,promise_date) >= 2 and DATEPART(HOUR,promise_date) < 4 then '2-4'
    when DATEPART(HOUR,promise_date) >= 4 and DATEPART(HOUR,promise_date) < 6 then '4-6'
    when DATEPART(HOUR,promise_date) >= 6 and DATEPART(HOUR,promise_date) < 8 then '6-8'
    when DATEPART(HOUR,promise_date) >= 8 and DATEPART(HOUR,promise_date) < 10 then '8-10'
    when DATEPART(HOUR,promise_date) >= 10 and DATEPART(HOUR,promise_date) < 12 then '10-12'
    when DATEPART(HOUR,promise_date) >= 12 and DATEPART(HOUR,promise_date) < 14 then '12-14'
    when DATEPART(HOUR,promise_date) >= 14 and DATEPART(HOUR,promise_date) < 16 then '14-16'
    when DATEPART(HOUR,promise_date) >= 16 and DATEPART(HOUR,promise_date) < 18 then '16-18'
    when DATEPART(HOUR,promise_date) >= 18 and DATEPART(HOUR,promise_date) < 20 then '18-20'
    when DATEPART(HOUR,promise_date) >= 20 and DATEPART(HOUR,promise_date) < 22 then '20-22'
    when DATEPART(HOUR,promise_date) >= 22 then '22-0'
    end as "Unloading slot",
    tonnage as "Weight (Kg)",
    no_of_cases as "Total no of cases planned in PO",
    total_jpin as "Total JPINs in PO",
    case when category_name = 'Oil, Ghee and Vanaspati' then 'OIL_CARTONS'
         when category_name = 'Sugar and Jaggery' and max(dead_weight) over (partition by order_id) >= 50 then 'SUGAR_50KG_BAG'
         when max(dead_weight) over (partition by order_id) >= 50 then '50KG_BAGS'
         when max(dead_weight) over (partition by order_id) < 50 and max(dead_weight) over (partition by order_id) >= 25 then '25KG_BAGS'
         when max(dead_weight) over (partition by order_id) < 25 and max(dead_weight) over (partition by order_id) >= 10 then '10KG_BAGS'
         when max(dead_weight) over (partition by order_id) < 10 and max(dead_weight) over (partition by order_id) >= 5 then '5KG_BAGS' 
         when max(dead_weight) over (partition by order_id) < 5 then 'FMCG_BOXES' 
    end as "Type of Material"
    from
    (
        select distinct
        pr.updated_promise_time as promise_date,
        isnull(pa.created_time_,ord.order_placed_timestamp) as order_placed_timestamp_,
        ord.order_item_id,
        count(isnull(prod.jpin,rpo.jpin)) over (partition by order_id) as total_jpin,
        ord.order_id,
        f.name as facility,
        ord.last_updated_by as po_raised_by,
        vs.vendor_display_name as vendor_name,
        vs.vendor_id,
        c.city_name,
        case when CASE
              WHEN l2_case_size IS NULL or l2_case_size=0 THEN l1_case_size
              WHEN l2_case_size IS NOT NULL THEN l2_case_size
              ELSE 1
              END =0 then 1 else CASE
              WHEN l2_case_size IS NULL or l2_case_size=0 THEN l1_case_size
              WHEN l2_case_size IS NOT NULL THEN l2_case_size
              ELSE 1
              END end AS case_size_,
        sku.dead_weight,
        sku.volumetric_weight,
        ord.quantity,
        (sum(ord.quantity*1.00/case_size_) over (partition by order_id)) as no_of_cases,
        (sum(ord.quantity*sku.dead_weight) over (partition by order_id)) as tonnage,
        cat.category_name,
        row_number() over (partition by ord.order_id order by order_placed_timestamp_ asc) as rnk   
        from bolt_order_item_v2_snapshot ord
        left join promise_snapshot pr on pr.promised_entity_id = ord.order_item_id
        left join (select 
                   promise_entity_id,
                   max(timestamp 'epoch' + (created_time / 1000) * interval '1 second' + interval '5 hours 30 minutes') as created_time_
                   from dw_promise.promise_audit
                   -- where promise_entity_id = 'BLTORDITM-1343800364'
                   group by 1) pa on pa.promise_entity_id = ord.order_item_id
        left join listing_snapshot ls on ls.listing_id = ord.listing_id
        left join sellerproduct_snapshot sps on sps.sp_id = isnull(ls.sp_id, ls.channel_spid)
        left join hevo_product prod on prod.jpin = sps.jpin
        left join hevo_replenishmentpurchaseordermetadata rpo on rpo.orderitemid = ord.order_item_id
        left join category cat on cat.pvid = prod.pvid
        left join dw_bradman.facility f on f.natural_id = rpo.facilityid
        left join vendor_snapshot vs on vs.org_profile_id = ord.seller_id
        left join dw_address.city c on c.natural_id = f.city_id
        left join org_profile_snapshot org on ord.buyer_id = org.org_profile_id
        left join dw_bradman.stock_keeping_unit_case_size_info sku on sku.sku_entity_id = isnull(prod.jpin,rpo.jpin) and sku.facility_id = f.natural_id and sku.status = 'VALID'
        where ord.boltordertype = 'REPLENISH'
        and ord.rporeasoncode != 'AUTO_PO'
        and date(pr.updated_promise_time) = current_date + 1
        and order_item_status != 'Cancelled'
        --and ord.order_id = 'BLTORD-1377820821'
        -- and [f.name=facility_name]
        and org.org_name in ('Amolakchand Ankur Kothari Enterprises Private Limited','Bodega Retail Private Limited',
      'Jumbotail Wholesale Private Limited','SourcingBee Retail Pvt Ltd','Tailhub Private Limited')
     -- group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
    )b
    where b.rnk = 1
--group by 1,2,3,4,5,6,b.tonnage,b.jpin,b.no_of_cases,b.dead_weight,b.category_name,b.promise_date 
)asn
left join hevo_jp_inbound_capacities_capacities cap on cap.fc_name = asn."FC-Name"
group by 1,2,4,6
order by "FC-Name"
"""

#%%
df = pd.read_sql(query,engine)

# %% Statples Data

sheet = gc.open('FC Capacity Visibility') # Inwarding data sheet

wks_1 = sheet.worksheet_by_title('ASN For Tomorrow')

wks_1.clear(start ='A1',end='Z')

wks_1.set_dataframe(df,start='A1')
# %%
