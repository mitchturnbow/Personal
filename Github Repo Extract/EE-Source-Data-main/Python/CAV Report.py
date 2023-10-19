import pyodbc
import pandas as pd
import datetime
import os
import win32com.client
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
import smtplib
# REGION 1 SET VARIABLES

#New Addition
ecprod = 'DSN=ECPROD'
connection = 'DSN=ECPROD'
#Path = 'C://Users/e010495/Documents/Test Folder'
Path = '//hmsfs/hmsdalfile_TPL/Cost Avoidance/CAV Client Services and Yield/TX HCSC CAV Report/'
save_path = Path + datetime.datetime.now().strftime('%Y-%m-%d') +'/'
if not os.path.exists(save_path):
    os.mkdir(save_path)
Cli_842 = 'TPRR_EP!_' + datetime.datetime.now().strftime('%Y-%m-%d') + '.txt'
Cli_607 = 'TPRR_BCB_' + datetime.datetime.now().strftime('%Y-%m-%d') + '.txt'
attachment1 = Cli_607
attachment2 = Cli_842
email_dist = '//hmsfs/hmsdalfile_TPL/Cost Avoidance/CAV Client Services and Yield/TX HCSC CAV Report/TX_CAV_Report_Distribution_List.txt'
analyst_dist = '//hmsfs/hmsdalfile_TPL/Cost Avoidance/CAV Client Services and Yield/TX HCSC CAV Report/TX_CAV_Report_Analyst_Distribution_List.txt'
to_address = open(email_dist).readlines()
to_address_analyst = open(analyst_dist).readlines()
#from_address = 'alfredo.gonzalez@gainwelltechnologies.com'
from_addresses = 'ATM@hms.com'
# END REGION 1

# REGION 2 SQL QUERYS
print('Running SQL query for client 842')
sql = '''
--Client Code 842 El Paso
SELECT Distinct
       I.MA2_NUM AS "Client Patient Control Number",
       F.RECIP_MA_NUM,
       F.Recip_First_Nm AS "Client First Name",
       F.RECIP_LAST_NM AS "Client Last Name",
       C.CARRIER_NM AS "Insurance Company Name",
       F.POLICY_NUM AS "Subscriber Number",
       COALESCE (F.GROUP_NUM, '', F.ORIG_GROUP_NUM) AS "Group Number",
       F.PH_FIRST_NM AS "Subscriber First Name",
       F.PH_LAST_NM AS "Subscriber Last Name",
       F.PH_SSN_NUM AS "Subscriber SSN",
       --Populates with states relationship code
       CASE When F.Rshcd_Rf = '03' Then 'C'
            When F.Rshcd_Rf = '02' Then 'M'
            When F.Rshcd_Rf = '09' Then 'O'
            When F.Rshcd_Rf = '01' Then 'S' End As Relationship,
        --Populates with states coverage type code
       Case When F.Client_Cd = '842' and F.Plctp_Rf = 'MHOnly' Then 'B'
            When F.Client_Cd = '842' and F.Plctp_Rf In ('EPO','HMO','HSA','MCMCO','OTH','POS','PPO') Then 'C'
            When F.Client_Cd = '842' and F.Plctp_Rf = 'RXOnly' Then 'K'
            When F.Client_Cd = '607' and F.Plctp_Rf = 'HMO' Then 'X'
            When F.Client_Cd = '607' and F.Plctp_Rf In ('EPO','MHONLY','OTH','POS','PPO') Then 'Y' End AS "Type Of Coverage",            
       F.POLICY_START_DT AS "Coverage Start Date",
       F.POLICY_END_DT AS "Coverage End Date",  
       F.EMP_NM AS "Employer/Union Name",
       --Populates with identifier for source instead of client code
       Case When F.Client_Cd = '842' Then 'EP1'
            When F.Client_Cd = '607' Then 'BCB' End As "Source Code"     
FROM FUS.ARTFUSP F
       LEFT JOIN CAR.ARTCARM_BASE C
          ON F.CARRIER_CD = C.CARRIER_CD
          AND F.CARRIER_OFFICE_CD = C.CARRIER_OFFICE_CD
       LEFT JOIN IND.ARTINDV_BASE I
          ON F.INDV_ID = I.INDV_ID AND F.CLIENT_CD = I.CLIENT_CD
--Select El Paso records delivered to client
WHERE (F.CLIENT_CD = '842'
        AND F.VRFRV_RF = '110'
        AND F.REPORT_DT BETWEEN (DATE (CURRENT DATE - (DAY (CURRENT DATE) - 1) DAYS - 1 MONTH)) AND (CURRENT DATE - (DAY(CURRENT DATE))) -- UPDATED FOR BETWEEN FIRST/LAST OF PREVIOUS MONTH
        AND C.DEFAULT_IND = 'Y')
WITH UR;
'''
print('Finished SQL query for client 842')
print('Running SQL query for client 607')
sql2 = '''
--Client Code 607 Blue Cross Blue Shield
SELECT Distinct
       I.MA2_NUM AS "Client Patient Control Number",
       F.RECIP_MA_NUM,
       F.Recip_First_Nm AS "Client First Name",
       F.RECIP_LAST_NM AS "Client Last Name",
       C.CARRIER_NM AS "Insurance Company Name",
       F.POLICY_NUM AS "Subscriber Number",
       COALESCE (F.GROUP_NUM, '', F.ORIG_GROUP_NUM) AS "Group Number",
       F.PH_FIRST_NM AS "Subscriber First Name",
       F.PH_LAST_NM AS "Subscriber Last Name",
       F.PH_SSN_NUM AS "Subscriber SSN",
       --Populates with states relationship code
       CASE When F.Rshcd_Rf = '03' Then 'C'
            When F.Rshcd_Rf = '02' Then 'M'
            When F.Rshcd_Rf = '09' Then 'O'
            When F.Rshcd_Rf = '01' Then 'S' End As Relationship,
        --Populates with states coverage type code
       Case When F.Client_Cd = '842' and F.Plctp_Rf = 'MHOnly' Then 'B'
            When F.Client_Cd = '842' and F.Plctp_Rf In ('EPO','HMO','HSA','MCMCO','OTH','POS','PPO') Then 'C'
            When F.Client_Cd = '842' and F.Plctp_Rf = 'RXOnly' Then 'K'
            When F.Client_Cd = '607' and F.Plctp_Rf = 'HMO' Then 'X'
            When F.Client_Cd = '607' and F.Plctp_Rf In ('EPO','MHONLY','OTH','POS','PPO') Then 'Y' End AS "Type Of Coverage",
       F.POLICY_START_DT AS "Coverage Start Date",
       F.POLICY_END_DT AS "Coverage End Date",
       F.EMP_NM AS "Employer/Union Name",
       --Populates with identifier for source instead of client code
       Case When F.Client_Cd = '842' Then 'EP1'
            When F.Client_Cd = '607' Then 'BCB' End As "Source Code"
 FROM FUS.ARTFUSP F
       LEFT JOIN CAR.ARTCARM_BASE C
          ON F.CARRIER_CD = C.CARRIER_CD
          AND F.CARRIER_OFFICE_CD = C.CARRIER_OFFICE_CD
       LEFT JOIN IND.ARTINDV_BASE I
          ON F.INDV_ID = I.INDV_ID AND F.CLIENT_CD = I.CLIENT_CD

 WHERE
        --Select BCBC Records excluding RX claims delivered to client.
        (F.CLIENT_CD = '607' AND (F.Plctp_Rf) NOT IN ('RXONLY') AND F.VRFRV_RF = '110'
        AND F.REPORT_DT BETWEEN (DATE (CURRENT DATE - (DAY (CURRENT DATE) - 1) DAYS - 1 MONTH)) AND (CURRENT DATE - (DAY(CURRENT DATE))) -- UPDATED FOR BETWEEN FIRST/LAST OF PREVIOUS MONTH 
        AND C.DEFAULT_IND = 'Y')
 WITH UR;
'''
print('Finished SQL query for client 607')

# END REGION 2

# REGION 3 CREATE SQL FUNCTION
def get_qry(sql,connection): #FUNCTION TO CREATE PANDAS DF FROM IBM DB2.
    sql_connection = pyodbc.connect(connection, autocommit=True, unicode_results=True) #IMPORT PYODBC PACKAGE WITH CONNECT FUNCTION
    result = pd.read_sql_query(sql,sql_connection)
    sql_connection.close()
    return result
# END REGION 3

# REGION 4 SET SQL VARIABLES PART 2
Client_842= get_qry(sql,connection)
print('1')
Client_607=get_qry(sql2,connection)
print('2')
Client_842.to_csv(save_path + Cli_842,sep="|",index=False)
print('3')
Client_607.to_csv(save_path + Cli_607,sep="|",index=False)
print('4')
# END REGION 4
def email_2a_out(from_address, to_address, subject, body, attachment_path, attachment_name1, attachment_name2):
    mail_content = f"""Subject: {subject} \n\n
    {body}
        """

    # The mail addresses and password
    server_name = 'smtp.hms.hmsy.com'

    # Setup the MIME
    message = MIMEMultipart()
    message['From'] = from_address
    message['To'] = '; '.join(to_address).replace("\n", "")
    message['Subject'] = subject
    # The subject line
    # The body and the attachments for the mail
    message.attach(MIMEText(mail_content))  # 'plain'
    ## Attachment 1
    # attach_file_name = attachment_name
    attach_file = open(attachment_path + attachment_name1, 'rb')  # Open the file as binary mode
    payload = MIMEBase('application', 'octate-stream')
    payload.set_payload(attach_file.read())
    # encoders.encode_base64(payload)  # encode the attachment
    # add payload header with filename
    payload.add_header('Content-Disposition', "attachment; filename= %s" % attachment_name1)
    print('Message Attachment Name:attachment, filename=' + attachment_name1)
    message.attach(payload)
    ## Attachment 2
    # attach_file_name = attachment_name
    attach_file = open(attachment_path + attachment_name2, 'rb')  # Open the file as binary mode
    payload = MIMEBase('application', 'octate-stream')
    payload.set_payload(attach_file.read())
    # encoders.encode_base64(payload)  # encode the attachment
    # add payload header with filename
    payload.add_header('Content-Disposition', "attachment; filename= %s" % attachment_name2)
    print('Message Attachment Name:attachment, filename=' + attachment_name2)
    message.attach(payload)
    # Create SMTP session for sending the mail
    server = smtplib.SMTP(server_name)
    text = message.as_string()
    server.sendmail(from_address, to_address, text)
    server.quit()
# REGION 5 SET EMAIL FUNCTION
# def email_2a_out(from_address, to_address, subject, body, attach1, attach2):
#
#     outlook = win32com.client.Dispatch('outlook.application')
#     mail = outlook.CreateItem(0)
#     mail.Sender = from_address
#     mail.To = '; '.join(to_address).replace("\n", "")
#     mail.Subject = subject
#     mail.Body = body
#     mail.Attachments.Add(save_path + attach1)
#     mail.Attachments.Add(save_path + attach2)
#     mail.Send()
# END REGION 5

# REGION 6 IF STATEMENTS FOR EMAIL TYPES
if Client_607.shape == (0,16) and Client_842.shape == (0,16):
    print('No Data for either clients')
    No_Data_Body = 'Both clients, El Paso First and Blue Cross Blue Shield does not have any new data for ' + datetime.datetime.now().strftime('%B %Y') + '. Email is forwarded to analyst for additional review.'
    Email_subject = 'TX CAV Reports - ' + datetime.datetime.now().strftime("%B %Y")
    email_2a_out(from_addresses, to_address_analyst, Email_subject, No_Data_Body, save_path, attachment1, attachment2)
elif Client_607.shape == (0,16) and Client_842.shape != (0,16):
    Client_607_No_Data_Body = 'Results for El Paso is attached.\n' \
                              ' No new data for Blue Cross Blue Shield for ' + datetime.datetime.now().strftime('%B %Y') + '.\n' \
                                                                                                                           ' Email is forwarded to analyst for additional review.'
    Email_subject = 'TX CAV Reports - ' + datetime.datetime.now().strftime("%B %Y")
    email_2a_out(from_addresses,to_address_analyst,Email_subject, Client_607_No_Data_Body, save_path,attachment1,attachment2)
    print('No Data for Blue Cross Blue Shield')
elif Client_607.shape != (0,16) and Client_842.shape == (0,16):
    Client_842_No_Data_Body = 'Results for Blue Cross Blue Shield is attached.\n' \
                              'No new data for El Paso for ' + datetime.datetime.now().strftime('%B %Y') + '.\n' \
                                                                                                           'Email is forwarded to analyst for additional review.'
    Email_subject = 'TX CAV Reports - ' + datetime.datetime.now().strftime("%B %Y")
    email_2a_out(from_addresses,to_address_analyst,Email_subject,Client_842_No_Data_Body,save_path,attachment1,attachment2)
    print('No Data for El Paso')
else:
    Data_Subject = 'TX CAV Reports - ' + datetime.datetime.now().strftime("%B %Y")
    Data_Body = 'TX ' + datetime.datetime.now().strftime('%B %Y') + ' CAV files for El Paso and Blue Cross Blue Shield attached.'
    email_2a_out(from_addresses,to_address,Data_Subject,Data_Body,save_path,attachment1,attachment2)
    print('El Paso & Blue Cross Blue Shield contain updated data')
    print(Client_607.shape)
    print(Client_842.shape)
# # END REGION 6


