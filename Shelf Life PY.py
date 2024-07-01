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
# %%
df = pd.read_sql(query,engine)
print(df)

# %% open Shelf life request sheet and transfers data from query to sheet

sheet = gc.open('Shelf Life Request Sheet')

wks2 = sheet.worksheet_by_title('Inventory Data')

wks2.set_dataframe(df,start='A1')

# %% opens jpin otb sheet

sh = gc.open('JPIN_OTB_MAIN_COPY')

wks_otb = sh.worksheet_by_title('Consolidated_Limits')

selected_columns = ['key',	'CITY','CITY Id',	'JPIN',	'title','category_name','max_drr']

otb_df = wks_otb.get_as_df()

# %% Only Select desired column

otb_df_selected = otb_df[selected_columns]

# %% Transfers data from above sheet to shelf life request sheet in jpin category work sheet

sheet = gc.open('Shelf Life Request Sheet')

wks3 = sheet.worksheet_by_title('Jpin Category')

wks3.clear()

wks3.set_dataframe(otb_df_selected,start='A1')

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

query ="""select 
distinct 
ps.jpin,
ps.title,
--br.brandid as BrandID,
--br.displaytitle as BrandName,
--ps.pvid,
pv.pvname,
--ps.imageurl1 as "ImageURL1",
--ps.imageurl2 as "ImageURL2",
--ps.imageurl3 as "ImageURL3",
--ps.imageurl4 as "ImageURL4",
--ps.catalogstatus,
--ps.StatusRemark,
-- json_extract_path_text(pvattributes,'StatusRemark') as "StatusRemark",
--json_extract_path_text(pvattributes,'Net_Weight') as "Net_Weight",
--json_extract_path_text(pvattributes,'Net_Weight_Measuring_Unit') as "Net_Weight_Measuring_Unit",
--json_extract_path_text(pvattributes,'Packaging Type') as "Packaging_Type",
--json_extract_path_text(pvattributes,'Net_Quantity') as "Net_Quantity",
--json_extract_path_text(pvattributes,'Net Quantity Measuring Unit') as "Net_Quantity_Measuring_Unit",
--json_extract_path_text(pvattributes,'Sub Product Vertical') as "Sub_Product_Vertical",
--json_extract_path_text(pvattributes,'Packaging_Classification') as "Packaging_Classification",
--json_extract_path_text(pvattributes,'MRP_Relevance') as "MRP_Relevance",
--json_extract_path_text(pvattributes,'Sub_Product_Vertical') as "Sub_Product_Vertical",
-- json_extract_path_text(pvattributes,'Quality Variant') as "Quality_Variant",

/*scm.deadweight as "Dead_Weight",
scm.volumetricweight as "Volumetric_Weight",
scm.length_l0 as "length_l0",
scm.width_l0 as "width_l0",
scm.height_l0 as "height_l0",
br.displaylogo as Brandlogo,*/
--br.manufacturerid as ManufacturerID,
--m.manufacturername as ManufacturerName,
--ps.business_category_id,
cat.category_name,
/*Business Category Name*/
case when ps.jpin in 
 (
   select distinct entity_id
from business_group_entity_exclusivity_snapshot be
left join business_group_snapshot_ bs on bs.bzidgrpid=be.bzgrp_id
left join bg__city_mapping bg on bg.businessgroupid=bzidgrpid
 where be.exclusive=1 and businessgroupid is not null
and bzidgrpname!='Yard seller to Yard seller'
   ) then 1 else 0 end as isb2b, 
 --json_extract_path_text(pvattributes,'CanRePack') as "CanRePack",
--json_extract_path_text(pvattributes,'Shelf Life Period') as "Shelf_Life_Period",
-- json_extract_path_text(pvattributes,'Shelf Life Limit') as "Shelf_Life_Limit",
--ps.srccreatedtime as CreatedTime,
--ps.srclastupdatedtime as lastupdatedtime,
hp.lastupdatedby as LastUpdatedBy
FROM product_snapshot_ ps
  LEFT JOIN productline_snapshot_ pl ON pl.productlineid = ps.productlineid
  LEFT JOIN category cat ON ps.pvid = cat.pvid
  left join category_snapshot_ c on c.categoryid=ps.business_category_id
  LEFT JOIN brand_snapshot_ br ON br.brandid = ps.brandid
       LEFT JOIN manufacturer_snapshot m ON m.manufacturerid = br.manufacturerid
  LEFT JOIN (SELECT DISTINCT (pvid), pvname FROM productvertical_snapshot_) pv ON pv.pvid = ps.pvid
  LEFT JOIN hevo_product hp ON hp.jpin = ps.jpin
  LEFT JOIN supplychainattributes_snapshot_ scm ON scm.jpin = ps.jpin
"""
# %%
df = pd.read_sql(query,engine)
print(df)
# %%
sheet_n = gc.open('Shelf Life Request Sheet')

wks3 = sheet_n.worksheet_by_title('Product Dump')

wks3.set_dataframe(df,start='A1')

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
query = """select P.jpin,p.title,--m.MANUFACTURERNAME,
Case
when p.title like '%%Sunflower%%' then 'Sunflower oil'
when p.title like '%%Palm%%' then 'Palm oil'
when p.title like '%%Mustard%%' then 'Mustard oil'
when p.title like '%%Groundnut%%' then 'Groundnut oil'
when p.title like '%%Ghee%%' then 'Ghee'
when p.title like '%%Coconut%%' then 'Coconut oil'
when p.title like '%%Deepaa%%' then 'Dheepam oil'
when p.title like '%%Refined Oil%%' then 'Blended oil'
when p.title like '%%Dheepa%%' then 'Dheepam oil'
when p.title like '%%Vanas%%' then 'Vanaspati'
when p.title like '%%Vegetable%%' then 'Blended oil'
when p.title like '%%Rice%%' then 'Rice bran oil'
when p.title like '%%Castor%%' then 'Castor oil'
when p.title like '%%Gingelly%%' then 'Gingelly oil'
when p.title like '%%Bakery%%' then 'Vanaspati'
when p.title like '%%Soya%%' then 'Soybean oil'
when p.title like '%%Olive%%' then 'Olive oil'
when p.title like '%%Deep%%' then 'Dheepam oil'
when p.title like '%%Cotton%%' then 'Cottonseed oil'
when p.title like '%%Lamp%%' then 'Dheepam oil'
when p.title like '%%Pooja%%' then 'Dheepam oil'
when p.title like '%%Saffola%%' then 'Blended oil'
when p.title like '%%Sesame%%' then 'Sesame oil'
when p.title like '%%Neem%%' then 'Neem oil'
when p.title like '%%Canola%%' then 'Canola oil'
when p.title like '%%Soybean%%' then 'Soybean oil'
when p.title like '%%Corn%%' then 'Corn oil'
when p.title like '%%Superolein%%' then 'Superolein oil'
when p.title like '%%Cottenseed%%' then 'Cottenseed oil'
when p.title like '%%Almonds%%' then 'Almonds oil'
when p.title like '%%Mustrad%%' then 'Mustard oil'
when p.title like '%%Salad%%' then 'Salad oil'
when p.title like '%%Copra%%' then 'Coconut oil'
when p.title like '%%Gingelly%%' then 'Gingelly oil'
when p.title like '%%Safflower%%' then 'Safflower oil'
when p.title like '%%Walnut%%' then 'Walnut oil'
when p.title like '%%Coconad%%' then 'Coconut oil'
when p.title like '%%Basil%%' then 'Basil oil'
when p.title like '%%Niger%%' then 'Niger seed oil'
when p.title like '%%Pomace%%' then 'Olive oil'
when p.title like '%%Kardai%%' then 'Safflower oil'
when p.title like '%%Til%%' then 'Til oil'
when p.title like '%%Cooking%%' then 'Blended oil'
ELSE p.title END AS PV,
--B.displaytitle as brand_name,S.L1_case_size,s.L2_case_size,s.deadweight,s.deadweight*0.6 AS SHIPPING_CHARGE, 
PV.pvname
from product_snapshot_ P
join brand_snapshot_ B ON P.BRANDID=B.BRANDID
LEFT join manufacturer_snapshot M ON m.MANUFACTURERID=B.MANUFACTURERID
LEFT join supplychainattributes_snapshot_ S ON p.jpin=S.jpin
LEFT JOIN category cat on cat.pvid = p.pvid
LEFT JOIN productvertical_snapshot_ PV ON PV.PVID =P.PVID
--where  m.MANUFACTURERNAME in ('B.L Agro Industries Ltd.','B.L Agro Industries limited')
where cat.category_name in ('Oil, Ghee and Vanaspati')
"""

# %%
df_oil = pd.read_sql(query,engine)
print(df_oil)

# %% Oil Pv data

oil_Sh = gc.open('Shelf Life Request Sheet')

oil_pv_wks = oil_Sh.worksheet_by_title('Oil Pvs')

oil_pv_wks.set_dataframe(df_oil,start='A1')
# %%

open_po = gc.open('New Planning Working BizOps')

open_po_sh = open_po.worksheet_by_title('Open PO Dump:RPO')

po_num_df = open_po_sh.get_as_df()

# print(po_num_df)

# %%

shelf_po = gc.open('Shelf Life Request Sheet')

po_shelf = shelf_po.worksheet_by_title('Open Po')

po_shelf.set_dataframe(po_num_df,start='A1' )
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
