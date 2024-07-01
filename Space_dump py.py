# %%
import pygsheets
import pandas as pd
import numpy as np
from datetime import datetime,timedelta

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
space_query = """select distinct
f.name as facility
, spq.product_id as JPIN
, spq.max_allowed_qty
, spq.current_free_qty
, spq.max_allowed_qty - spq.current_free_qty as occupied_qty
, inv.sellable_inv
, spq.reserve_qty_for_put
from
dw_bradman.space_product_qty_map spq
left join dw_bradman.space s on
s.id = spq.space_id
left join dw_bradman.infrastructure infra on
infra.id = s.infra_id
left join (
select
product_id
, space_id
, sum(
case
when state = 'SELLABLE'
then left_qty
end
)
as sellable_inv
, sum(
case
when state = 'FULFILMENT'
then left_qty
end
)
as order_received_inv
from
dw_bradman.inventory_item ii
where
state in (
'SELLABLE'
, 'FULFILMENT'
)
and status in (
'ACTIVE'
)
group by
1
, 2
)
inv on
inv.product_id = spq.product_id
and inv.space_id = s.id
left join product_snapshot_ p on
p.jpin = spq.product_id
left join hevo_productvertical pv on
p.pvid = pv.pvid
left join category cat on
pv.pvid = cat.pvid
left join supplychainattributes_snapshot_ sca on
sca.jpin = p.jpin
left join dw_bradman.facility f on
f.id = s.facility_id
where
spq.association_status = 'ACTIVE'
and spq.purpose = 'STORAGE'
and spq.capacity_type = 'PRODUCT'
and s.space_usage_purpose = 'STORAGE'
and s.is_leaf_space = 1
and s.space_status = 'ACTIVE'
and f.name in (
'Sutlej/Gomati',
'FC-Ahmedabad',
'FC-Bhubaneswar',
'FC-Chandigarh',
'FC-Chennai',
'FC-Coimbatore',
'FC-Faridabad',
'FC-Ghaziabad',
'FC-Gurgaon',
'FC-Hyderabad',
'FC-Jaipur',
'FC-Lucknow',
'FC-Ludhiana',
'FC-Patna',
'FC-Pune',
'FC-Ranchi',
'FC-Trichy',
'FC-Vijayawada',
'Vajra',
'Vayu',
'Vikrant')

order by
1 asc
, 3 asc"""

space_df = pd.read_sql(space_query,engine)

space_df = space_df.fillna('')

#%%

space_sheet = gc.open('Rationalization_Dashboard RK')

space_dump = space_sheet.worksheet_by_title('Space Dump')

space_dump.set_dataframe(space_df,start='A1')


# %%
