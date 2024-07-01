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

desired_columns = ['Date','Facility Name','Jpin' ,'Title','Po Qty In Units','DOC (If Rejected)','New DOC (If Accpeted)','Received Shelf Life','Status','Approval Needed From','Accepted/Rejected','Vendor name','Po Number','Inwarded/Not Inwarded']

# %%
filtered_data = yesterday_data[desired_columns]

# %% define function for conditional formating

# define counters

counters = {'approved_inwarded': 0, 'rejected_not_inwarded': 0, 'rejected_inwarded': 0, 'approved_not_inwarded':0}

# Define the conditional formatting function


def conditional_formatting_status(row):

    status = row['Status'].lower()
    accepted_rejected = row['Accepted/Rejected'].lower()
    inwarded_not_inwarded = row['Inwarded/Not Inwarded'].lower()
    
    if (status == 'approved' or accepted_rejected == 'accepted') and inwarded_not_inwarded == 'inwarded':
       counters['approved_inwarded'] += 1
       return ['background-color: lightgreen'] * len(row)
    
    elif (status == 'rejected' or accepted_rejected == 'rejected') and inwarded_not_inwarded == 'not inwarded':
        counters['rejected_not_inwarded'] += 1
        return ['background-color: yellow'] * len(row)
    
    elif (status == 'rejected' or accepted_rejected == 'rejected') and inwarded_not_inwarded == 'inwarded':
        counters['rejected_inwarded'] += 1
        return ['background-color: red'] * len(row)
    
    elif (status == 'approved' or accepted_rejected == 'accepted') and inwarded_not_inwarded == 'not inwarded':
        counters['approved_not_inwarded'] += 1
        return ['background-color: orange'] * len(row)
    
    else:
        return [''] * len(row)


#%% apply conditional formating to status column
filtered_data.reset_index(drop=True, inplace=True)

filtered_data.index += 1

filtered_data_styled = filtered_data.style.apply(conditional_formatting_status, axis=1).applymap(border).set_properties(**{'text-align': 'center'})

total_count = filtered_data.shape[0]

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


receiver = 'akshay.shetter@jumbotail.com','business.fmcg@jumbotail.com','businessoperations@jumbotail.com','avm_staple@jumbotail.com','avm_fmcg@jumbotail.com','rakesh.jadhav@jumbotail.com','srinivas.hn@jumbotail.com','sunil.kumar@jumbotail.com','mallikarjun.patil@jumbotail.com','vivekanand.mathapati@jumbotail.com','mouli.tirugabathini@jumbotail.com'

subject = 'Shelf Life Approvals Requested Data Dated: {}'.format(yesterday_date.strftime('%d-%m-%Y'))


body1 = f"""
Yesterday {total_count} JPINS were requested for Shelf Life Approvals.<br><br>
In which:<br><br>
 1).  {counters['approved_inwarded']} JPINS were Approved and Inwarded.(Highlighted in Green)<br><br>
 2).  {counters['rejected_not_inwarded']} JPINS were Rejected and Not Inwarded.(Highlighted in Yellow)<br><br>
 3).  {counters['rejected_inwarded']} JPINS were Rejected but Inwarded.(Highlighted in Red)<br><br>
 4).  {counters['approved_not_inwarded']} JPINS were Accepted but Not Inwarded.(Highlighted in Orange)<br><br>
"""


body5 = """<br><br> Best Regards,<br>
Akshay Shetter<br>
Senior Executive-Business Operations<br>
Mobile : +916363884575<br><br> """

cc = 'akshay.shetter@jumbotail.com','saikrishna.kakumani@jumbotail.com ','rohit.dande@jumbotail.com','kamlesh.kumar@jumbotail.com','purushothama.hs@jumbotail.com'

attachment1 = "C:/Users/Dell/Pictures/Saved Pictures/shelf_life.png"

em = EmailMessage()
em['From']=sender
em['To']=receiver
em['Subject']=subject
em['Cc']=cc
em.set_content(body1)
attachment_cid1 = make_msgid()


em.set_content('<b style="font-size:15px;">{}</b><img src="cid:{}"/><br/><br/><br/>{}'.format(body1, attachment_cid1[1:-1], body5), 'html')


with open(attachment1, 'rb') as fp1:
    em.add_related(fp1.read(), 'image', 'png', cid=attachment_cid1)

with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
    smtp.login(sender, password)
        
    smtp.send_message(em)

    # %%
