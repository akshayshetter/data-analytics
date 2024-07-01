# %%
import pygsheets
import pandas as pd 
import numpy as np
from datetime import datetime,timedelta

gc = pygsheets.authorize(service_file='C:/Users/Dell/Documents/Python Scripts/Note/test-api-403913-222895678b66.json')

shelf_sheet = gc.open('Shelf Life Request Sheet')
shelf_wks = shelf_sheet.worksheet_by_title('Shelf life Backup')

df = shelf_wks.get_as_df()

# convert date column to date format if not already done

def border(cell_value):
   return "border: solid thin"

df['Date'] = pd.to_datetime(df['Date']) 

# calcutlate yesterday's date

yesterday = datetime.now() - timedelta(days=1)

yesterday_date = yesterday.date()

# filter only yesterday data

yesterday_data = df[df['Date'].dt.date == yesterday_date]

desired_columns = ['Date','Facility Name','Jpin' ,'Title','Po Qty In Units','DOC (If Rejected)','New DOC (If Accpeted)','Received Shelf Life','Status','Approval Needed From','Accepted/Rejected','Vendor name','Po Number']

# %%
filtered_data = yesterday_data[desired_columns]

# %% define function for conditional formating

def conditional_formating_status(status):
 
 if status == 'KAM Approval Needed':
    return 'background-color: yellow'

 elif status == 'Rejected':
    return 'background-color: red'

 elif status == 'Approved':
    return 'background-color: green'

 else:
    return ''


#%% apply conditional formating to status column
filtered_data.reset_index(drop=True, inplace=True)

filtered_data.index += 1

filtered_data_styled = filtered_data.style.apply({'Status': lambda x: conditional_formating_status(x)})

# %%
styled_df = filtered_data_styled.set_table_styles([
    {'selector': 'thead th', 'props': [('text-align', 'center')]},  # Align headers at the center
    {'selector': 'td', 'props': [('text-align', 'center'), ('border', '1px solid black')]},  # Align cell values at the center and add a border
])

# %%
print(styled_df)

#%%
## convcert data frame to image shelf life

import dataframe_image as dfi

dfi.export(styled_df,"C:/Users/Dell/Pictures/Saved Pictures/shelf_life.png")
#%% approved data
shelf_sheet = gc.open('Shelf Life Request Sheet')

shelf_wks = shelf_sheet.worksheet_by_title('Shelf life Backup')

df = shelf_wks.get_as_df()

def border(cell_value):
   return "border: solid thin"

df['Date'] = pd.to_datetime(df['Date']) 

yesterday = datetime.now() - timedelta(days=1)

yesterday_date = yesterday.date()

yesterday_date_data = df[df['Date'].dt.date == yesterday_date]

desired_columns = ['Date','Facility Name','Jpin' ,'Title','Po Qty In Units','Received Shelf Life','Status','Accepted/Rejected','Vendor name','Po Number','Inwarded/Not Inwarded']

yesterday_df = yesterday_date_data[desired_columns]
                                   
yesterday_df = yesterday_df[(yesterday_df['Status'] == 'Approved') | (yesterday_df['Accepted/Rejected'] == 'Accepted')]

yesterday_df.reset_index(drop=True, inplace=True)

yesterday_df.index += 1

styled_df1 = yesterday_df.style.applymap(border).set_table_styles([
    {'selector': 'thead th', 'props': [('text-align', 'center')]},  # Align headers at the center
    {'selector': 'td', 'props': [('text-align', 'center'), ('border', '1px solid black')]},  # Align cell values at the center and add a border
])

print(styled_df1)

# %%
## convcert data frame to image

import dataframe_image as dfi

dfi.export(styled_df1,"C:/Users/Dell/Pictures/Saved Pictures/shelf_life_approved.png")

# %% rejected jpins
shelf_sheet = gc.open('Shelf Life Request Sheet')

shelf_wks = shelf_sheet.worksheet_by_title('Shelf life Backup')

df = shelf_wks.get_as_df()

def border(cell_value):
   return "border: solid thin"

df['Date'] = pd.to_datetime(df['Date']) 

yesterday = datetime.now() - timedelta(days=1)

yesterday_date = yesterday.date()

yesterday_date_data = df[df['Date'].dt.date == yesterday_date]

desired_columns = ['Date','Facility Name','Jpin' ,'Title','Po Qty In Units','Received Shelf Life','Status','Accepted/Rejected','Vendor name','Po Number','Inwarded/Not Inwarded']

yesterday_df = yesterday_date_data[desired_columns]
                                   
yesterday_df = yesterday_df[((yesterday_df['Status'] == 'Rejected') | (yesterday_df['Accepted/Rejected'] == 'Rejected')) & (yesterday_df['Inwarded/Not Inwarded'] == 'Not Inwarded')]

yesterday_df.reset_index(drop=True, inplace=True)

yesterday_df.index += 1

styled_df2 = yesterday_df.style.applymap(border).set_table_styles([
    {'selector': 'thead th', 'props': [('text-align', 'center')]},  # Align headers at the center
    {'selector': 'td', 'props': [('text-align', 'center'), ('border', '1px solid black')]},  # Align cell values at the center and add a border
])

print(styled_df2)

# %%
## convcert data frame to image

import dataframe_image as dfi

dfi.export(styled_df2,"C:/Users/Dell/Pictures/Saved Pictures/shelf_life_rejected.png")

# %% rejected and inwarded
shelf_sheet = gc.open('Shelf Life Request Sheet')

shelf_wks = shelf_sheet.worksheet_by_title('Shelf life Backup')

df = shelf_wks.get_as_df()

def border(cell_value):
   return "border: solid thin"

df['Date'] = pd.to_datetime(df['Date']) 

yesterday = datetime.now() - timedelta(days=1)

yesterday_date = yesterday.date()

yesterday_date_data = df[df['Date'].dt.date == yesterday_date]

desired_columns = ['Date','Facility Name','Jpin' ,'Title','Po Qty In Units','Received Shelf Life','Status','Accepted/Rejected','Vendor name','Po Number','Inwarded/Not Inwarded']

yesterday_df = yesterday_date_data[desired_columns]
                                   
yesterday_df = yesterday_df[((yesterday_df['Status'] == 'Rejected') | (yesterday_df['Accepted/Rejected'] == 'Rejected')) & (yesterday_df['Inwarded/Not Inwarded'] == 'Inwarded')]

yesterday_df.reset_index(drop=True, inplace=True)

yesterday_df.index += 1

styled_df3 = yesterday_df.style.applymap(border).set_table_styles([
    {'selector': 'thead th', 'props': [('text-align', 'center')]},  # Align headers at the center
    {'selector': 'td', 'props': [('text-align', 'center'), ('border', '1px solid black')]},  # Align cell values at the center and add a border
])

print(styled_df3)

# %%
## convcert data frame to image

import dataframe_image as dfi

dfi.export(styled_df3,"C:/Users/Dell/Pictures/Saved Pictures/shelf_life_rejected_inwarded.png")
# %%
# mail script

from email.message import EmailMessage
import ssl
import smtplib
import imghdr 

from email.utils import make_msgid

# sender
sender = "akshay.shetter@jumbotail.com"

# password
password = "dsog zama fxqo niug"


receiver = 'akshay.shetter@jumbotail.com',##'saikrishna.kakumani@jumbotail.com '

subject = 'shelf life approvals'

body1 = """
PFA list of jpins accpeted or rejected yesterday due to shelf life issues.<br><br>"""

body2 = """
PFA list of Jpins with Shelf Life Issues which are Approved and Inwarded.<br><br>"""

body3 = """
PFA list of Jpins with Shelf Life Issues which are Rejected and Not Inwarded.<br><br>"""

body4 = """
PFA list of Jpins with Shelf Life Issues which are Rejected and Inwarded.<br><br>"""

body5 = """<br><br> Best Regards,<br>
Akshay Shetter<br>
Senior Executive-Business Operations<br>
Mobile : +916363884575<br><br> """

cc = 'akshay.shetter@jumbotail.com'

attachment1 = "C:/Users/Dell/Pictures/Saved Pictures/shelf_life.png"

attachment2 = "C:/Users/Dell/Pictures/Saved Pictures/shelf_life_approved.png"

attachment3 = "C:/Users/Dell/Pictures/Saved Pictures/shelf_life_rejected.png"

attachment4 = "C:/Users/Dell/Pictures/Saved Pictures/shelf_life_rejected_inwarded.png"

em = EmailMessage()
em['From']=sender
em['To']=receiver
em['Subject']=subject
em['Cc']=cc
em.set_content(body1)
attachment_cid1 = make_msgid()
attachment_cid2 = make_msgid()
attachment_cid3 = make_msgid()
attachment_cid4 = make_msgid()


em.set_content('<b style="font-size:15px;">%s</b><img src="cid:%s"/><br/><br/><br/><b style="font-size:15px;">%s</b><img src="cid:%s"/><br/><br/><br/><b style="font-size:15px;">%s</b><img src="cid:%s"/><br/><br/><br/><b style="font-size:15px;">%s</b><img src="cid:%s"/><b>%s</b>' % (body1, attachment_cid1[1:-1], body2, attachment_cid2[1:-1], body3, attachment_cid3[1:-1], body4, attachment_cid4[1:-1], body5), 'html')

with open(attachment1, 'rb') as fp1:
    em.add_related(fp1.read(), 'image', 'png', cid=attachment_cid1)

with open(attachment2, 'rb') as fp2:
    em.add_related(fp2.read(), 'image', 'png', cid=attachment_cid2)

with open(attachment3, 'rb') as fp3:
    em.add_related(fp3.read(), 'image', 'png', cid=attachment_cid3)

with open(attachment4, 'rb') as fp4:
    em.add_related(fp4.read(), 'image', 'png', cid=attachment_cid4)    

with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
    smtp.login(sender, password)
        
    smtp.send_message(em)

#break
# %% bucket waise data
##import pygsheets
##import pandas as pd
##import numpy as np
##from datetime import datetime,timedelta

##gc = pygsheets.authorize(service_file='C:/Users/Dell/Documents/Python Scripts/Note/test-api-403913-222895678b66.json')

#%% approved data
##shelf_sheet = gc.open('Shelf Life Request Sheet')

##shelf_wks = shelf_sheet.worksheet_by_title('Shelf life Backup')

##df = shelf_wks.get_as_df()

##df['Date'] = pd.to_datetime(df['Date']) 

##yesterday = datetime.now() - timedelta(days=1)

##yesterday_date = yesterday.date()

##yesterday_date_data = df[df['Date'].dt.date == yesterday_date]

##desired_columns = ['Date','Facility Name','Jpin' ,'Title','Po Qty In Units','Received Shelf Life','Status','Accpeted/Rejected','Po Number','Inwarded/Not Inwarded']

##yesterday_df = yesterday_date_data[desired_columns]
                                   
##yesterday_df = yesterday_df[(yesterday_df['Status'] == 'Approved') | (yesterday_df['Accpeted/Rejected'] == 'Accepted')]

##print(yesterday_df)

# %%
## convcert data frame to image

##import dataframe_image as dfi

##dfi.export(yesterday_df,"C:/Users/Dell/Pictures/Saved Pictures/shelf_life_approved.png")

# %% rejected 
##shelf_sheet = gc.open('Shelf Life Request Sheet')

##shelf_wks = shelf_sheet.worksheet_by_title('Shelf life Backup')

##df = shelf_wks.get_as_df()

##df['Date'] = pd.to_datetime(df['Date']) 

##yesterday = datetime.now() - timedelta(days=1)

##yesterday_date = yesterday.date()

##yesterday_date_data = df[df['Date'].dt.date == yesterday_date]

##desired_columns = ['Date','Facility Name','Jpin' ,'Title','Po Qty In Units','Received Shelf Life','Status','Accpeted/Rejected','Po Number','Inwarded/Not Inwarded']

##yesterday_df = yesterday_date_data[desired_columns]
                                   
##yesterday_df = yesterday_df[((yesterday_df['Status'] == 'Rejected') | (yesterday_df['Accpeted/Rejected'] == 'Rejected')) & (yesterday_df['Inwarded/Not Inwarded'] == 'Not Inwarded')]

##print(yesterday_df)

# %%
## convcert data frame to image

##import dataframe_image as dfi

##dfi.export(yesterday_df,"C:/Users/Dell/Pictures/Saved Pictures/shelf_life_rejected.png")

# %% rejected and inwarded
##shelf_sheet = gc.open('Shelf Life Request Sheet')

##shelf_wks = shelf_sheet.worksheet_by_title('Shelf life Backup')

##df = shelf_wks.get_as_df()

##df['Date'] = pd.to_datetime(df['Date']) 

##yesterday = datetime.now() - timedelta(days=1)

##yesterday_date = yesterday.date()

##yesterday_date_data = df[df['Date'].dt.date == yesterday_date]

##desired_columns = ['Date','Facility Name','Jpin' ,'Title','Po Qty In Units','Received Shelf Life','Status','Accpeted/Rejected','Po Number','Inwarded/Not Inwarded']

##yesterday_df = yesterday_date_data[desired_columns]
                                   
##yesterday_df = yesterday_df[((yesterday_df['Status'] == 'Rejected') | (yesterday_df['Accpeted/Rejected'] == 'Rejected')) & (yesterday_df['Inwarded/Not Inwarded'] == 'Inwarded')]

##print(yesterday_df)

# %%
## convcert data frame to image

##import dataframe_image as dfi

##dfi.export(yesterday_df,"C:/Users/Dell/Pictures/Saved Pictures/shelf_life_rejected_inwarded.png")
# %%
