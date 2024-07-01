#%%
import pandas as pd
import numpy as np
import pygsheets
from datetime import datetime,timedelta

gc = pygsheets.authorize(service_file='C:/Users/Dell/Documents/Python Scripts/Note/test-api-403913-222895678b66.json')

#%%
# Open backup file

sh = gc.open_by_key('1_c2QsdGeJbJEvMO4kRAjDvWxfuZqkdE4LnLM1jF9Rfw')

bkp = sh.worksheet_by_title('Final Summary')

bkp_df = bkp.get_as_df(start='R1',end='Y9')

bkp_df_new = bkp.get_as_df(start='R20',end='Y28')
#%%
def border(cell_value):
    return "border: solid thin"
#%%

bkp_df['Date'] = pd.to_datetime(bkp_df['Date'],format = '%d-%m-%Y')

bkp_df_new['Date'] = pd.to_datetime(bkp_df_new['Date'],format = '%d-%m-%Y')

yesterday = datetime.now()-timedelta(days=1)

yesterday_date =yesterday.date()

#%%
#filetr only yesterday date
#%%
yesterday_data = bkp_df[bkp_df['Date'].dt.strftime('%d-%m-%Y') == yesterday_date.strftime('%d-%m-%Y')]
#%%
yesterday_data2 = bkp_df_new[bkp_df_new['Date'].dt.strftime('%d-%m-%Y')==yesterday_date.strftime('%d-%m-%Y')]
#%%
req_col = [	'City',	'#Jpins System Suggested',	'#Jpins Avm Planned', '#Jpins Po Raised','#Jpins Delivered','% Of Jpins Planned vs System Suggested','% Of Jpins Delivered vs Jpins Po Raised'	]
#%%
#req_col['% Of Jpins Planned vs System Suggested'] = req_col['% Of Jpins Planned vs System Suggested'].str[:-1]
#%%
req_col2 = ['City','#Total Cases Suggested By System',	'#Cases AVM Planned',	'#Cases Po Raised',	'#Cases Delivered', '% Of Cases Avm Planned vs Cases System Suggested','% Of Cases Delivered vs Cases Po raised']
#%%
bkp_df2 = yesterday_data[req_col]
#%%
bkp_df3 = yesterday_data2[req_col2]

#%%
def to_int(x):
        x = float("{:.2f}".format(x))
        return (str(x))
def df_style(val):
        return "font-weight: bold"
def cond_formatting(x):
        if x >= 90:
            return 'background-color: lightgreen'
        elif (x >= 60) & (x <90):
            return 'background-color: yellow'
        elif x < 60:
            return 'background-color: red'
        else:
            return None
# %%
    #percentage columns

#percentage_columns = ['% Of Jpins Planned vs System Suggested','% Of Jpins Delivered vs Jpins Po Raised']

#bkp_df2[percentage_columns] = bkp_df2[percentage_columns].applymap(format_percentage)
        
#%%
bkp_df2.reset_index(drop=True, inplace=True)

bkp_df3.reset_index(drop=True,inplace=True)

bkp_df2.index +=1
bkp_df3.index +=1

#%%

temp3=bkp_df2.style.applymap(border).applymap(cond_formatting,subset=['% Of Jpins Planned vs System Suggested','% Of Jpins Delivered vs Jpins Po Raised']).set_properties(**{'text-align': 'center'})

temp4=bkp_df3.style.applymap(border).applymap(cond_formatting,subset=['% Of Cases Avm Planned vs Cases System Suggested','% Of Cases Delivered vs Cases Po raised']).set_properties(**{'text-align': 'center'})
#%%
print(temp3)
#%%
print(temp4)
# %%
# count of jpins,

#result_df = bkp_df2.groupby('City').sum()

#%%
#temp3.reset_index(drop=True, inplace=True)

#temp4.reset_index(drop=True,inplace=True)

temp3.index +=1
temp4.index +=1
# %%
print(temp3)
#%%
print(temp4)
# %%
#styled_df = bkp_df2.style.map(border).set_properties(**{'text-align':'center'})
#%%
#styled_df2 = bkp_df3.style.map(border).set_properties(**{'text-align':'center'})
#%%
#styled_bkp = styled_df.set_table_styles([{'selector': 'thead th', 'props': [('text-align', 'center')]} #aligns header at center
#,{'selector': 'td', 'props': [('text-align', 'center'), ('border', '1px solid black')]},]) # align text at center
#%%
#styled_bkp2 = styled_df2.set_table_styles([{'selector': 'thead th', 'props': [('text-align', 'center')]} #aligns header at center
#,{'selector': 'td', 'props': [('text-align', 'center'), ('border', '1px solid black')]},]) # align text at center



#%%
import dataframe_image as dfi
#%%
dfi.export(temp3,"C:/Users/Dell/Pictures/Saved Pictures/oil_summary.png")
#%%

dfi.export(temp4,"C:/Users/Dell/Pictures/Saved Pictures/oil_cases_summary.png")
# %%

#%% 
# opne backup sheet

bkp_new = sh.worksheet_by_title('Final Summary')

bkp_new2 = bkp_new.get_as_df(start='A1',end='M')

columns = ['City',	'JPIN',	'Title','System Suggested Cases',	'Avm Planned Cases',	'Po Raised Cases',	'Delivered Cases']

bkp_new2 = bkp_new2[columns]
#%%
# Convert columns to numeric type
bkp_new2['System Suggested Cases'] = pd.to_numeric(bkp_new2['System Suggested Cases'], errors='coerce')
bkp_new2['Avm Planned Cases'] = pd.to_numeric(bkp_new2['Avm Planned Cases'], errors='coerce')

#%%
bkp_new2 = bkp_new2[(bkp_new2['Avm Planned Cases'] >= bkp_new2['System Suggested Cases']) |  (bkp_new2['Avm Planned Cases'] <= bkp_new2['System Suggested Cases'])]

#bkp_new2 = bkp_new2[bkp_new2['System Suggested Cases'] >= 0]

#%%
#%%
#bkp_new2.reset_index(drop=False, inplace=True)

#bkp_new2.index

#%%
print(bkp_new2)

styled_df2 = bkp_new2.style.set_properties(**{'text-align':'center'})

styled_bkp2 = styled_df2.set_table_styles([{'selector': 'thead th', 'props': [('text-align', 'center')]} #aligns header at center
,{'selector': 'td', 'props': [('text-align', 'center'), ('border', '1px solid black')]},]) # align text at center


# %%
# export df to exel file
styled_bkp2.to_excel('C:/Users/Dell/Documents/Oil Files/Daily_Planning_Summary.xlsx',index=False)
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


receiver = 'akshay.shetter@jumbotail.com',#'business.fmcg@jumbotail.com','businessoperations@jumbotail.com','avm_staple@jumbotail.com','avm_fmcg@jumbotail.com','rakesh.jadhav@jumbotail.com','srinivas.hn@jumbotail.com','sunil.kumar@jumbotail.com','mallikarjun.patil@jumbotail.com','vivekanand.mathapati@jumbotail.com','mouli.tirugabathini@jumbotail.com'

subject = 'Oil Planinng Summary: {}'.format(yesterday_date.strftime('%d-%m-%Y'))

body1 = f"""
Hi Team,<br>

Please find the Oil,Ghe and Vanaspati category Planning summary for yesterday across all locations.<br> 
Detailed Jpins list is attached below for reference.<br><br>
"""
body2 = """
Jpins Summary <br>"""

body3 = """
Cases Summary <br>"""


body5 = """<br><br> Best Regards,<br>
Akshay Shetter<br>
Senior Executive-Business Operations<br>
Mobile : +916363884575<br><br> """

cc = 'akshay.shetter@jumbotail.com',#'saikrishna.kakumani@jumbotail.com ','rohit.dande@jumbotail.com',#'kamlesh.kumar@jumbotail.com','purushothama.hs@jumbotail.com'

attachment1 = "C:/Users/Dell/Pictures/Saved Pictures/oil_summary.png"

attachment2 = "C:/Users/Dell/Pictures/Saved Pictures/oil_cases_summary.png"


#attachment2 ="C:/Users/Dell/Documents/Oil Files/Daily_Requirement_Summary.xlsx"

em = EmailMessage()
em['From']=sender
em['To']=receiver
em['Subject']=subject
em['Cc']=cc
em.set_content(body1)
attachment_cid1 = make_msgid()
attachment_cid2 = make_msgid()

em.set_content('<b>%s</b><b>%s</b><br/><img src="cid:%s"/><br/><b>%s</b><br/><img src="cid:%s"/><br/><b>%s</b>' % (body1,body2, attachment_cid1[1:-1],body3,attachment_cid2[1:-1],body5), 'html')

with open(attachment1, 'rb') as fp:
    em.add_related(fp.read(), 'image', 'png', cid=attachment_cid1)

with open(attachment2, 'rb') as fp2:
    em.add_related(fp2.read(), 'image', 'png', cid=attachment_cid2)    

with open(f'C:/Users/Dell/Documents/Oil Files/Daily_Planning_Summary.xlsx','rb') as f:
    file_data = f.read()
    em.add_attachment(file_data, maintype="application", subtype="xlsx", filename=f'Daily_Planning_Summary.xlsx')

    context1=ssl.create_default_context()

with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context1) as smtp:
    smtp.login(sender, password)

    smtp.send_message(em)
    #break
# %%
