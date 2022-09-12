# Work in Progress
# Recreating Tableau Data Source through Python, including calculated fields
# Includes: 
# 	- Appeanding calcuated fields onto end of a Pandas Data Frame
# 	- How to replicate FIXED calculation and append to DF
# 		df['New Field Name'] = join.groupby('Fixed Field')['Target Field'].transform('Aggregate Function (min, max, etc)')
# DB2 SQL Function not currently being used
# User/Password for SQL Server query function have been removed

import pandas as pd
import pyodbc
import datetime
import os

#################### Set Path ####################
path = 'C:/Users/mturnbow/OneDrive - Gainwell Technologies/Desktop/Data Lake Data Sources Test/'
full_path = path + datetime.datetime.today().strftime('%m-%d-%Y') + '/'
if not os.path.exists(full_path):
    os.mkdir(full_path)

report_date = datetime.datetime.today().strftime('%m/%d/%Y')

#################### Run SQL Functions/Output CSV ####################
# def run_db2sql(dsn, sql, file_name):
#     con = pyodbc.connect('DSN=' + dsn)
#     df = pd.read_sql_query(sql,con)
#     df.to_csv(full_path + file_name, index=False)
#     con.close()
#     return df

def run_sqlserver(server, database, sql, file_name):
    conn_str = (
            r'Driver=SQL Server;'
            r'Server=' + server + ';'
            r'Database=' + database + ';'
            r'UID=;'
            r'PWD=;'
    )
    con = pyodbc.connect(conn_str)
    df = pd.read_sql_query(sql, con)
    df.to_csv(full_path + file_name, index=False)
    con.close()
    return df

#################### Calculated Fields ####################
def profile_file_status(join):
    if join['EVENT_ID_FROM_JSON'] == join['EVENT_ID']:
        return 1
    elif join['EVENT_ID_FROM_JSON'] == '' and join['EVENT_ID'] == '':
        return 2
    elif join['EVENT_ID_FROM_JSON'] == '' and join['EVENT_ID'] != '':
        return 3
    else:
        return 4

def group_file_status(join):
    agg_group_status = join['Group_File_Status_Cd']
    if agg_group_status == 1:
        return 'Good'
    elif agg_group_status == 2:
        return 'No File History/Group Has Never Completed'
    elif agg_group_status == 3:
        return 'File(s) in B2B But Group Has Never Completed'
    elif agg_group_status == 4:
        return 'File(s) in B2B But No gPM Load'
    else:
        return 'ERROR'

def file_sla_status(join):
    day_dif = abs((datetime.datetime.today() - join['Max_Date']).days)
    if join['DLFC_GROUP_FREQUENCY'].lower() == 'annualy':
        if day_dif > 375:
            return 3
        else:
            return 1
    elif join['DLFC_GROUP_FREQUENCY'].lower() == 'semi annual':
        if day_dif > 185:
            return 3
        else:
            return 1
    elif join['DLFC_GROUP_FREQUENCY'].lower() == 'quarterly':
        if day_dif > 94:
            return 3
        else:
            return 1
    elif join['DLFC_GROUP_FREQUENCY'].lower() == 'monthly':
        if day_dif > 34:
            return 3
        else:
            return 1
    elif join['DLFC_GROUP_FREQUENCY'].lower() == 'weekly':
        if day_dif > 11:
            return 3
        else:
            return 1
    elif join['DLFC_GROUP_FREQUENCY'].lower() == 'daily':
        if day_dif > 4:
            return 3
        else:
            return 1
    else:
        return 2

def group_sla_status(join):
    agg_group_status = join['Group_SLA_Status_Cd']
    if agg_group_status == 1:
        return 'Good'
    elif agg_group_status == 2:
        return 'ERROR'
    elif agg_group_status == 3:
        return 'SLA Breach'
    else:
        return 'error'

def new_creation(join):
    rec_insert_date_diff = abs((datetime.datetime.today() - join['Max_Rec_Insert_Time']).days)
    if rec_insert_date_diff <= 30:
        return 1
    else:
        return 0

def group_previously_completed(join):
    # profile_previously_completed_sum = join.groupby('DLGC_GROUPING_ID')['Profile_Previously_Completed'].sum()
    if pd.isnull(join['DLEFC_GROUPING_LAST_DT']) :
        return 0
    else:
        return 1

def group_status(join):
    if join['Group_File_Status_Nm'] == 'Good' and join['Group_SLA_Status_Nm'] == 'Good':
        return 'Good'
    elif join['Group_Previously_Completed'] == 0 and join['New_Creation_Flag'] == 1:
        return 'Issue Type 5'
    elif join['Group_Previously_Completed'] == 0:
        return 'Issue Type 4'
    elif (join['Group_File_Status_Nm'] == 'File(s) in B2B But No gPM Load' or join['Group_File_Status_Nm'] == 'File(s) in B2B But Group Has Never Completed') and join['Group_SLA_Status_Nm'] == 'Good':
        return 'Issue Type 1'
    elif join['Group_File_Status_Nm'] == 'Good' and join['Group_SLA_Status_Nm'] == 'SLA Breach':
        return 'Issue Type 2'
    elif join['Group_SLA_Status_Nm'] == 'SLA Breach' or join['Group_SLA_Status_Nm'] == 'ERROR':
        return 'Issue Type 3'
    else:
        return ''

#################### Run Queries ####################
dlsql = """
with cte1 AS (
select * from 
(select DLFG_GROUPING_ID, DLEFC_GROUPING_SEQ_NUMBER, DLEGC_GROUPING_STATUS, DLEGC_GROUPING_START_DT, DLEGC_GROUPING_END_DT, DLEFC_GROUPING_LAST_DT,
 DLEGC_GROUPING_CONSUMPTION_STATUS, DLEGC_GROUPING_FILE_COUNT_RECEIVED, DLEGC_GROUPING_FILE_JSON, DLEGC_GROUPING_FILE_COUNT_EXPECTED, DLEGC_GROUPING_MAX_FL_FREQ, 
ROW_NUMBER() OVER(PARTITION BY DLFG_GROUPING_ID ORDER BY DLEFC_GROUPING_LAST_DT DESC) AS ROW_NUM
from [dbo].[DL_EIM_GROUPING_CONTROL] 
where DLEGC_GROUPING_STATUS = 'DL_GROUP_COMPLETED' and DLEGC_GROUPING_CONSUMPTION_STATUS = 'CONSUMPTION_COMPLETED') tbl1
where ROW_NUM = 1),
tbl2 AS (Select DLFG_GROUPING_ID, DLEFC_GROUPING_SEQ_NUMBER, DLEGC_GROUPING_STATUS, DLEGC_GROUPING_START_DT, DLEGC_GROUPING_END_DT, DLEFC_GROUPING_LAST_DT,
DLEGC_GROUPING_CONSUMPTION_STATUS, DLEGC_GROUPING_FILE_COUNT_RECEIVED, DLEGC_GROUPING_FILE_COUNT_EXPECTED, DLEGC_GROUPING_MAX_FL_FREQ,
event_id, profile_name 
from cte1 
CROSS APPLY OPENJSON(DLEGC_GROUPING_FILE_JSON, '$.fileDetails')
WITH (event_id NVARCHAR(50) '$.eventIdB2B', profile_name NVARCHAR(100) '$.profileNameB2B'))
select GC.[DLFG_GROUP_CRE_USR], GC.[DLFG_REC_INSERT_TIME], GC.[DLFG_ENTERPRISE_PARTNER_NAME], GC.[DLFG_GROUP_NAME], 
       GC.[DLGC_GROUPING_ID], GC.[DLFG_GROUP_ACTV_IND], GC.[DLFG_GROUP_LOADPROCESS], GC.[DLFC_GROUP_FREQUENCY], GC.[DLFG_TWS_APP_NAME], 
	   GC.[DLFG_SINGLE_FILE_CONSUMPTION_INDICATOR], GC.[DLFG_FILE_GROUP_COUNT], XW.DLFX_B2B_FILE_PROFILE_NAME,--GC.[DLFC_GROUP_DESCRIPTION],
	   TC.DLEFC_GROUPING_SEQ_NUMBER, TC.DLEGC_GROUPING_STATUS, --TC.DLEGC_GROUPING_START_DT, TC.DLEGC_GROUPING_END_DT,
       TC.DLEGC_GROUPING_CONSUMPTION_STATUS, TC.DLEGC_GROUPING_FILE_COUNT_RECEIVED, TC.DLEGC_GROUPING_FILE_COUNT_EXPECTED,
	   --TC.DLEGC_GROUPING_MAX_FL_FREQ,
	   TC.event_id as EVENT_ID_FROM_JSON, TC.profile_name as PROFILE_NAME_FROM_JSON, TC.DLEFC_GROUPING_LAST_DT,
	   FC.[DLFC_FILE_NAME], FC.[DLFC_DLK_LZ_PATH], FC.[DLFC_PARTNER_NAME], 
	   --FC.[DLFC_STATUS], FC.[DLFC_START_DTM], FC.[DLFC_END_DTM], FC.[DLFC_CONFIG_ID], FC.[DLFC_LOB], FC.[DLFC_FREQUENCY],IC.[DLIC_B2B_FILE_DOMAIN], 
	   IC.[DLIC_B2B_FILE_PATTERN], IC.[DLIC_DLK_FILE_DATA_TYPE]
FROM [dbo].[DL_FILE_GROUPING_CONFIG] GC
left join [dbo].[DL_EIM_GROUPING_FILE_XWALK] XW
	ON GC.DLGC_GROUPING_ID=XW.DLGC_GROUPING_ID
left join tbl2 TC
	on GC.[DLGC_GROUPING_ID] = TC.DLFG_GROUPING_ID and
	TC.profile_name = XW.DLFX_B2B_FILE_PROFILE_NAME
left join [dbo].[DL_FILE_CONTROL] FC
	on TC.event_id = FC.[DLFC_B2B_EVENT_ID]  
left join [dbo].[DL_INGEST_CONFIG_2] IC
	on TC.profile_name = IC.DLIC_B2B_FILE_PROFILE_NAME
"""
print('Running DL SQL...')
dlres = run_sqlserver('wpdbeim001', 'EIM_DB', dlsql, 'DL.csv')
dlres['DL_B2B_Profile_Match'] = dlres['DLFX_B2B_FILE_PROFILE_NAME'].str.replace(' ','')
dlres['DL_B2B_Profile_Match'] = dlres['DL_B2B_Profile_Match'].str.lower()
print('DL done...\n')

b2bsql = """
select *
from(
SELECT 
INBDMD.EVENT_ID,
INBDMD.PARTNER_NM as B2B_PARTNER_NAME,
--INBDMD.PROFILE_ID,
INBDMD.PROFILE_NAME,
--INBDMD.ACCOUNT_NM,
INBDMD.ENTERPRISE_PARTNER_NAME as PARENT_PARTNER_NAME,
INBDMD.ENTERPRISE_PARTNER_ABBR as PARENT_PARTNER_ABBR,
INBDMD.ENTERPRISE_PARTNER_CODE as PARENT_PARTNER_CODE,
INBDMD.PARTNER_NAME as CHILD_PARTNER_NAME,
INBDMD.PARTNER_ABBR as CHILD_PARTNER_ABBR, 
INBDMD.PARTNER_CODE as CHILD_PARTNER_CODE,
--INBDMD.INBD_FILE_PATH,
INBDMD.INBD_FILE_NM,
INBDMD.ORGINAL_FILE_NM,
--INBDMD.FILE_DOMAIN,
--INBDMD.FILE_CONTENT_TYPE,
--INBDMD.FILE_COMPRESSION_TYPE,
INBDMD.FILE_REC_IN_DTM,
--INBDMD.FILE_REC_OUT_DTM,
--INBDMD.DEST_FILE_PATH,
INBDMD.DEST_FILE_NM,
--INBDMD.FILE_STATUS,
--INBDMD.INBD_LANDING_PATH,
--INBDMD.FREQUENCY,
--INBDMD.FILE_IND,
--TRADING_PARTNER_TYPE,
ROW_NUMBER () OVER (PARTITION BY INBDMD.PROFILE_NAME ORDER BY INBDMD.FILE_REC_IN_DTM DESC) AS EVENT_ORDER
FROM DBO.SRC_INBD_METADATA AS INBDMD
where INBDMD.PROFILE_NAME not in ('INBD_Boston_Medical_Ctr_BOSFWAELIG_Client Eligibility_File', 'INBD_Contra_Costa_Ccchpmed_Prov_Provider _File')
)A
where a.EVENT_ORDER ='1'
"""
print('Running B2B SQL...')
b2bres = run_sqlserver('wpdbb2b004', 'B2BClientFileMetaData', b2bsql, 'B2B Data.csv')
b2bres['B2B_Profile_Match'] = b2bres['PROFILE_NAME'].str.replace(' ','')
b2bres['B2B_Profile_Match'] = b2bres['B2B_Profile_Match'].str.lower()
print('B2B Data Done...\n')


#################### Join Results ####################
print('Joining results...')
join = dlres.merge(b2bres, how='left' , left_on='DL_B2B_Profile_Match', right_on='B2B_Profile_Match')
print('Join complete!')

#################### Append Calculated Fields ####################
join['Profile_File_Status_Cd'] = join.apply(profile_file_status, axis=1)
join['Group_File_Status_Cd'] = join.groupby('DLGC_GROUPING_ID')['Profile_File_Status_Cd'].transform('max')
join['Group_File_Status_Nm'] = join.apply(group_file_status, axis=1)
join['Max_Date'] = join.groupby('DLGC_GROUPING_ID')['FILE_REC_IN_DTM'].transform('max')
join['File_SLA_Status_Cd'] = join.apply(file_sla_status, axis=1)
join['Group_SLA_Status_Cd'] = join.groupby('DLGC_GROUPING_ID')['File_SLA_Status_Cd'].transform('max')
join['Group_SLA_Status_Nm'] = join.apply(group_sla_status, axis=1)
join['Max_Rec_Insert_Time'] = join.groupby('DLGC_GROUPING_ID')['DLFG_REC_INSERT_TIME'].transform('max')
join['New_Creation_Flag'] = join.apply(new_creation, axis=1)
join['Max_File_Count'] = join.groupby('DLGC_GROUPING_ID')['DLFG_FILE_GROUP_COUNT'].transform('max')
join['Group_Previously_Completed'] = join.apply(group_previously_completed, axis=1)
join['Group_Status'] = join.apply(group_status, axis=1)
join.to_csv(path + 'DL File Info.csv')

summary_df = join[['DLGC_GROUPING_ID','DLFG_GROUP_ACTV_IND','Group_Status']].copy()
summary_df = summary_df.drop_duplicates(subset=['DLGC_GROUPING_ID'])
summary_df['Report_Date'] = report_date
summary_df.to_csv(path + 'DL Summary Results.csv')
print(summary_df)
