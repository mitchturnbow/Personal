# Recreating Tableau Data Source through Python, including calculated fields
# Includes:
# 	- Appending calculated fields onto end of a Pandas Data Frame
# 	- How to replicate FIXED calculation and append to DF:
#      df['New Field Name'] = df.groupby('Fixed Field')['Target Field'].transform('Aggregate Function (min, max, etc.)')
# DB2 SQL Function not currently being used
# User/Password for SQL Server query function have been removed

import pandas as pd
import pyodbc
import datetime
import os
import xlwings as xw

# ################### Show the starting date/time ################### #
start_time = datetime.datetime.now()
dt_string = start_time.strftime("%m/%d/%Y %H:%M:%S.%f")
print(f"Start: {dt_string}")

# ################### Set Path ################### #
# path = 'C:/Users/plight/OneDrive - Gainwell Technologies/Desktop/Python Reports/'
path = '//hmsdalfile/general/EE Source Data/Business Integrations/Data Lake Consumption/Python Reports/'
full_path = path + datetime.datetime.today().strftime('%m-%d-%Y') + '/'
if not os.path.exists(full_path):
    os.mkdir(full_path)

report_date = datetime.datetime.today().strftime('%m/%d/%Y')

# ################## Set File Names ################## #
# Daily Run Files #
data_lake_query_results_file_name = 'DL.csv'
b2b_query_results_file_name = 'B2B Data.csv'
summary_file_name = 'DL Summary Results.csv'
full_info_file_name = 'DL File Info.csv'
flag_file_name = 'DL Flag Info.csv'

# History File Names #
full_history_file_name = "Flag History Report Full.csv"

# ## FILE PATH FOR POWER BI RELATED FILE --- REPLACE WITH MORE GENERIC IF POSSIBLE ## #
workbook_path = "C:/Users/plight/OneDrive - Gainwell Technologies/Documents/Testing Document Links/"

# Trending and Flag #
trending_file_name = "group trending.xlsx"
group_trending_sheet_name = 'Group Trending'
group_trending_table_name = 'Group_Trending_Table'
group_history_sheet_name = "Group Flag History Report"
group_history_table_name = 'Group_History_Table'

# ################### Run SQL Functions/Output CSV ################### #
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
            r'UID=svc_e007997;'
            r'PWD=9t2,E-*F+}F[]u?j'
    )
    con = pyodbc.connect(conn_str)
    df = pd.read_sql_query(sql, con)
    df.to_csv(full_path + file_name, index=False)
    con.close()
    return df


# ################### Calculated Fields ################### #
def profile_file_status(data_frame):
    if data_frame['EVENT_ID_FROM_JSON'] == data_frame['EVENT_ID']:
        return 1
    elif data_frame['EVENT_ID_FROM_JSON'] == '' and data_frame['EVENT_ID'] == '':
        return 2
    elif data_frame['EVENT_ID_FROM_JSON'] == '' and data_frame['EVENT_ID'] != '':
        return 3
    else:
        return 4


def group_file_status(data_frame):
    agg_group_status = data_frame['Group_File_Status_Cd']
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


def file_sla_status(data_frame):
    day_dif = abs((datetime.datetime.today() - data_frame['Max_Date']).days)
    if data_frame['DLFC_GROUP_FREQUENCY'].lower() == 'annually':
        if day_dif > 375:
            return 3
        else:
            return 1
    elif data_frame['DLFC_GROUP_FREQUENCY'].lower() == 'semi annual':
        if day_dif > 185:
            return 3
        else:
            return 1
    elif data_frame['DLFC_GROUP_FREQUENCY'].lower() == 'quarterly':
        if day_dif > 94:
            return 3
        else:
            return 1
    elif data_frame['DLFC_GROUP_FREQUENCY'].lower() == 'monthly':
        if day_dif > 34:
            return 3
        else:
            return 1
    elif data_frame['DLFC_GROUP_FREQUENCY'].lower() == 'weekly':
        if day_dif > 11:
            return 3
        else:
            return 1
    elif data_frame['DLFC_GROUP_FREQUENCY'].lower() == 'daily':
        if day_dif > 4:
            return 3
        else:
            return 1
    else:
        return 2


def group_sla_status(data_frame):
    agg_group_status = data_frame['Group_SLA_Status_Cd']
    if agg_group_status == 1:
        return 'Good'
    elif agg_group_status == 2:
        return 'ERROR'
    elif agg_group_status == 3:
        return 'SLA Breach'
    else:
        return 'error'


def new_creation(data_frame):
    rec_insert_date_diff = abs((datetime.datetime.today() - data_frame['Max_Rec_Insert_Time']).days)
    if rec_insert_date_diff <= 30:
        return 1
    else:
        return 0


def group_previously_completed(data_frame):
    # profile_previously_completed_sum = data_frame.groupby('DLGC_GROUPING_ID')['Profile_Previously_Completed'].sum()
    if pd.isnull(data_frame['DLEFC_GROUPING_LAST_DT']):
        return 0
    else:
        return 1


def group_status(data_frame):
    if data_frame['Group_File_Status_Nm'] == 'Good' and data_frame['Group_SLA_Status_Nm'] == 'Good':
        return 'Good'
    elif data_frame['Group_Previously_Completed'] == 0 and data_frame['New_Creation_Flag'] == 1:
        return 'Issue Type 5'
    elif data_frame['Group_Previously_Completed'] == 0:
        return 'Issue Type 4'
    elif (data_frame['Group_File_Status_Nm'] == 'File(s) in B2B But No gPM Load'
          or data_frame['Group_File_Status_Nm'] == 'File(s) in B2B But Group Has Never Completed') \
            and data_frame['Group_SLA_Status_Nm'] == 'Good':
        return 'Issue Type 1'
    elif data_frame['Group_File_Status_Nm'] == 'Good' and data_frame['Group_SLA_Status_Nm'] == 'SLA Breach':
        return 'Issue Type 2'
    elif data_frame['Group_SLA_Status_Nm'] == 'SLA Breach' or data_frame['Group_SLA_Status_Nm'] == 'ERROR':
        return 'Issue Type 3'
    else:
        return ''


def client_carrier(data_frame):
    if data_frame['DLFG_GROUP_LOADPROCESS'] == 'CARR' or 'carrier' in data_frame['DLFG_GROUP_NAME'].lower():
        return 'Carrier'
    else:
        return 'Client'


def frequency_update(data_frame):
    frequency = data_frame['DLFC_GROUP_FREQUENCY']
    frequency = frequency.replace(' ', '')
    frequency = frequency.replace('"', '')
    frequency = frequency.title()
    if frequency.endswith('ly') is False:
        frequency = frequency + 'ly'
    return frequency


# ################### Run Queries ################### #
dlsql = """
with cte1 AS (
select * from 
(select DLFG_GROUPING_ID, DLEFC_GROUPING_SEQ_NUMBER, DLEGC_GROUPING_STATUS, DLEGC_GROUPING_START_DT, 
    DLEGC_GROUPING_END_DT, DLEFC_GROUPING_LAST_DT, DLEGC_GROUPING_CONSUMPTION_STATUS, 
    DLEGC_GROUPING_FILE_COUNT_RECEIVED, DLEGC_GROUPING_FILE_JSON, DLEGC_GROUPING_FILE_COUNT_EXPECTED, 
    DLEGC_GROUPING_MAX_FL_FREQ, 
    ROW_NUMBER() OVER(PARTITION BY DLFG_GROUPING_ID ORDER BY DLEFC_GROUPING_LAST_DT DESC) AS ROW_NUM
from [dbo].[DL_EIM_GROUPING_CONTROL] 
where DLEGC_GROUPING_STATUS = 'DL_GROUP_COMPLETED' and DLEGC_GROUPING_CONSUMPTION_STATUS = 'CONSUMPTION_COMPLETED') tbl1
where ROW_NUM = 1),
tbl2 AS (Select DLFG_GROUPING_ID, DLEFC_GROUPING_SEQ_NUMBER, DLEGC_GROUPING_STATUS, DLEGC_GROUPING_START_DT, 
    DLEGC_GROUPING_END_DT, DLEFC_GROUPING_LAST_DT, DLEGC_GROUPING_CONSUMPTION_STATUS, 
    DLEGC_GROUPING_FILE_COUNT_RECEIVED, DLEGC_GROUPING_FILE_COUNT_EXPECTED, DLEGC_GROUPING_MAX_FL_FREQ,
    event_id, profile_name 
from cte1 
CROSS APPLY OPENJSON(DLEGC_GROUPING_FILE_JSON, '$.fileDetails')
WITH (event_id NVARCHAR(50) '$.eventIdB2B', profile_name NVARCHAR(100) '$.profileNameB2B'))
select GC.[DLFG_GROUP_CRE_USR], GC.[DLFG_REC_INSERT_TIME], GC.[DLFG_ENTERPRISE_PARTNER_NAME], GC.[DLFG_GROUP_NAME], 
    GC.[DLGC_GROUPING_ID], GC.[DLFG_GROUP_ACTV_IND], GC.[DLFG_GROUP_LOADPROCESS], GC.[DLFC_GROUP_FREQUENCY], 
    GC.[DLFG_TWS_APP_NAME], 
    GC.[DLFG_SINGLE_FILE_CONSUMPTION_INDICATOR], GC.[DLFG_FILE_GROUP_COUNT], XW.DLFX_B2B_FILE_PROFILE_NAME,
    --GC.[DLFC_GROUP_DESCRIPTION],
    TC.DLEFC_GROUPING_SEQ_NUMBER, TC.DLEGC_GROUPING_STATUS, --TC.DLEGC_GROUPING_START_DT, TC.DLEGC_GROUPING_END_DT,
    TC.DLEGC_GROUPING_CONSUMPTION_STATUS, TC.DLEGC_GROUPING_FILE_COUNT_RECEIVED, 
    TC.DLEGC_GROUPING_FILE_COUNT_EXPECTED,
    --TC.DLEGC_GROUPING_MAX_FL_FREQ,
    TC.event_id as EVENT_ID_FROM_JSON, TC.profile_name as PROFILE_NAME_FROM_JSON, TC.DLEFC_GROUPING_LAST_DT,
    FC.[DLFC_FILE_NAME], FC.[DLFC_DLK_LZ_PATH], FC.[DLFC_PARTNER_NAME], 
    --FC.[DLFC_STATUS], FC.[DLFC_START_DTM], FC.[DLFC_END_DTM], FC.[DLFC_CONFIG_ID], FC.[DLFC_LOB], FC.[DLFC_FREQUENCY], IC.[DLIC_B2B_FILE_DOMAIN], 
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
dlres = run_sqlserver('wpdbeim001', 'EIM_DB', dlsql, data_lake_query_results_file_name)
dlres['DL_B2B_Profile_Match'] = dlres['DLFX_B2B_FILE_PROFILE_NAME'].str.replace(' ', '')
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
where INBDMD.PROFILE_NAME not in ('INBD_Boston_Medical_Ctr_BOSFWAELIG_Client Eligibility_File', 
    'INBD_Contra_Costa_Ccchpmed_Prov_Provider _File')
)A
where a.EVENT_ORDER ='1'
"""
print('Running B2B SQL...')
b2bres = run_sqlserver('wpdbb2b004', 'B2BClientFileMetaData', b2bsql, b2b_query_results_file_name)
b2bres['B2B_Profile_Match'] = b2bres['PROFILE_NAME'].str.replace(' ', '')
b2bres['B2B_Profile_Match'] = b2bres['B2B_Profile_Match'].str.lower()
print('B2B Data Done...\n')

# ################### Join Results ################### #
print('Joining results...')
current_file_info_df = dlres.merge(b2bres, how='left', left_on='DL_B2B_Profile_Match', right_on='B2B_Profile_Match')
print('Join complete!')

# ################### Append Calculated Fields ################### #
current_file_info_df['Profile_File_Status_Cd'] = current_file_info_df.apply(profile_file_status, axis=1)
current_file_info_df['Group_File_Status_Cd'] = (
    current_file_info_df.groupby('DLGC_GROUPING_ID')['Profile_File_Status_Cd'].transform('max'))
current_file_info_df['Group_File_Status_Nm'] = current_file_info_df.apply(group_file_status, axis=1)
current_file_info_df['Max_Date'] = current_file_info_df.groupby('DLGC_GROUPING_ID')['FILE_REC_IN_DTM'].transform('max')
current_file_info_df['File_SLA_Status_Cd'] = current_file_info_df.apply(file_sla_status, axis=1)
current_file_info_df['Group_SLA_Status_Cd'] = (
    current_file_info_df.groupby('DLGC_GROUPING_ID')['File_SLA_Status_Cd'].transform('max'))
current_file_info_df['Group_SLA_Status_Nm'] = current_file_info_df.apply(group_sla_status, axis=1)
current_file_info_df['Max_Rec_Insert_Time'] = (
    current_file_info_df.groupby('DLGC_GROUPING_ID')['DLFG_REC_INSERT_TIME'].transform('max'))
current_file_info_df['New_Creation_Flag'] = current_file_info_df.apply(new_creation, axis=1)
current_file_info_df['Max_File_Count'] = (
    current_file_info_df.groupby('DLGC_GROUPING_ID')['DLFG_FILE_GROUP_COUNT'].transform('max'))
current_file_info_df['Group_Previously_Completed'] = current_file_info_df.apply(group_previously_completed, axis=1)
current_file_info_df['Group_Previously_Completed'] = (
    current_file_info_df.groupby('DLGC_GROUPING_ID')['Group_Previously_Completed'].transform('max'))  # Added in
current_file_info_df['Group_Status'] = current_file_info_df.apply(group_status, axis=1)
current_file_info_df.to_csv(path + full_info_file_name)
current_file_info_df.to_csv(full_path + full_info_file_name)

# ################### Create CSV for Current Summary ################### #
current_summary_df = current_file_info_df[['DLGC_GROUPING_ID', 'DLFG_GROUP_ACTV_IND', 'DLFC_GROUP_FREQUENCY',
                                           'DLFG_ENTERPRISE_PARTNER_NAME', 'PARENT_PARTNER_NAME', 'PARENT_PARTNER_ABBR',
                                           'CHILD_PARTNER_ABBR', 'DLFG_FILE_GROUP_COUNT', 'DLFG_GROUP_NAME',
                                           'PROFILE_NAME', 'EVENT_ID', 'FILE_REC_IN_DTM', 'DLEFC_GROUPING_SEQ_NUMBER',
                                           'EVENT_ID_FROM_JSON', 'DLEFC_GROUPING_LAST_DT', 'Max_Date',
                                           'Group_Status']].copy()
current_summary_df = current_summary_df.drop_duplicates()
current_summary_df['Report_Date'] = report_date
current_summary_df['Report_Date'] = pd.to_datetime(current_summary_df['Report_Date']).dt.strftime('%m/%d/%Y')
current_summary_df.to_csv(path + summary_file_name)
current_summary_df.to_csv(full_path + summary_file_name)
print('Summary Complete!')

# ############################################## Update New Trending ################################################ #
current_info_df = current_file_info_df.copy()

# #### Load Trending Data #### #
group_trending_workbook = xw.Book(workbook_path + trending_file_name)
print('File Opened')
group_trending_worksheet = group_trending_workbook.sheets[group_trending_sheet_name]
print("Sheet Found")
trending_df = group_trending_worksheet.range(group_trending_table_name + '[#All]').options(pd.DataFrame, header=1,
                                                                                           index=False).value
print('Trending Table into Dataframe')
print(trending_df)

# #### Check if it has already run today #### #
if trending_df[trending_df['Report Date'] == report_date].shape[0] > 0:
    trending_df = trending_df[trending_df['Report Date'] != report_date]

# #### Trending DF Columns (Calculation and Final #### #
initial_trending_columns = ['Report Date', 'DLFG_ENTERPRISE_PARTNER_NAME', 'DLFG_GROUP_NAME', 'DLGC_GROUPING_ID',
                            'DLFG_GROUP_ACTV_IND', 'DLFG_GROUP_LOADPROCESS', 'DLFC_GROUP_FREQUENCY',
                            'PARENT_PARTNER_NAME', 'PARENT_PARTNER_ABBR', 'PARENT_PARTNER_CODE', 'CHILD_PARTNER_NAME',
                            'CHILD_PARTNER_CODE', 'Group_Status']

trending_columns = ['Report Date', 'Client/Carrier', 'DLFG_ENTERPRISE_PARTNER_NAME', 'DLFG_GROUP_LOADPROCESS',
                    'DLFC_GROUP_FREQUENCY', 'Group_Status']

trending_columns_rename = ["Report Date", "Client/Carrier", "Data Lake Partner Name", "File Type", "Frequency",
                           "Group Status", "Count"]

# Add Report Date
current_info_df['Report Date'] = report_date

# Drop Unneeded columns
current_trending_df = current_info_df[initial_trending_columns].copy()
current_trending_df.drop_duplicates(inplace=True)

# #### Run Functions for Client/Carrier and Frequency #### #
current_trending_df['Client/Carrier'] = current_trending_df.apply(client_carrier, axis=1)
current_trending_df['DLFC_GROUP_FREQUENCY'] = current_trending_df.apply(frequency_update, axis=1)
current_trending_df = current_trending_df[current_trending_df.DLFG_GROUP_ACTV_IND == "Y"]

# #### Update Columns and Add Count #### #
current_trending_df = current_trending_df[trending_columns].copy()
current_trending_df = current_trending_df.groupby(current_trending_df.columns.tolist()).size().reset_index().rename(
    columns={0: 'Count'})
current_trending_df.drop_duplicates(inplace=True)

# #### Rename Columns #### #
current_trending_df.columns = trending_columns_rename

# Add to History Dataframe
trending_df = pd.concat([trending_df, current_trending_df])
trending_df.drop_duplicates(keep='last', inplace=True)
print("Data Added to Trending DF!")

# #### Load to Table #### #
trending_df["Report Date"] = pd.to_datetime(trending_df["Report Date"])
trending_df["Report Date"] = trending_df["Report Date"].dt.date
trending_df.sort_values(by=["Report Date"], ascending=False, inplace=True)
group_trending_worksheet.tables[group_trending_table_name].update(trending_df, index=False)
print('Trending Table Updated')

# ################### Prepare Current File Info ################### #
process_start_time = datetime.datetime.now()

current_file_info_df = current_file_info_df[current_file_info_df.DLFG_GROUP_ACTV_IND == "Y"]
current_file_info_df.drop_duplicates(inplace=True)

process_end_time = datetime.datetime.now()
print(f'Process 1 Time: {process_end_time - process_start_time}')

# #################### Load Previous Flag File #################### #
process_start_time = datetime.datetime.now()

n = 1
n_max = 14
while n < n_max:
    try:
        previous_day = datetime.date.today() + datetime.timedelta(days=-n)
        previous_files_full_path = path + previous_day.strftime('%m-%d-%Y') + '/'
        previous_flag_info_df = pd.read_csv(previous_files_full_path + flag_file_name, index_col=0)
        n = n_max
    except FileNotFoundError:
        n += 1

print(previous_flag_info_df)
previous_flag_info_df.drop_duplicates(inplace=True)
previous_flag_info_full_df = previous_flag_info_df.copy()

process_end_time = datetime.datetime.now()
print(f'Process 2 Time: {process_end_time - process_start_time}')

# ############# Create the Data Difference Variable ############# #
current_day = datetime.date.today()
date_delta = current_day - previous_day


# ######################## Flag Function ######################## #
def unique_flag_id(data_frame):
    if data_frame['Group_Status'] == 'Good':
        return "None"
    else:
        group_id = str(data_frame['DLGC_GROUPING_ID'])
        seq_num = str(data_frame['DLEFC_GROUPING_SEQ_NUMBER']).removesuffix('.0')
        if seq_num == 'None':
            seq_num = 'nan'
        b2b_profile = str(data_frame['DLFX_B2B_FILE_PROFILE_NAME'])
        event_id = str(data_frame['EVENT_ID_FROM_JSON']).removesuffix('.0')
        if event_id == 'None':
            event_id = 'nan'
        flag_id = 'Group ID - ' + group_id + ' | Seq Number - ' + seq_num + ' | B2B Profile - ' + b2b_profile +\
                  ' | JSON Event ID - ' + event_id
        return flag_id


# ################### Add Flag Columns to Current File Info DF and create New Flag Info DF ################### #
process_start_time = datetime.datetime.now()

current_file_info_df['Unique_Flag_ID'] = current_file_info_df.apply(unique_flag_id, axis=1)
new_flag_info_df = current_file_info_df.copy()

process_end_time = datetime.datetime.now()
print(f'Process 3 Time: {process_end_time - process_start_time}')

# ################### Drop Extra Columns from Flag DFs and remove Duplicates ################### #
process_start_time = datetime.datetime.now()

labels_to_drop = ['DLEGC_GROUPING_STATUS', 'DLEGC_GROUPING_CONSUMPTION_STATUS', 'DLEFC_GROUPING_LAST_DT',
                  'DLFG_GROUP_CRE_USR', 'DLFG_REC_INSERT_TIME', 'DLFG_SINGLE_FILE_CONSUMPTION_INDICATOR',
                  'File_SLA_Status_Cd', 'Group_SLA_Status_Cd', 'Group_SLA_Status_Nm', 'Max_Rec_Insert_Time',
                  'New_Creation_Flag', 'Max_File_Count', 'EVENT_ORDER', 'DLFG_GROUP_ACTV_IND', 'Profile_File_Status_Cd',
                  'Group_File_Status_Cd', 'Group_File_Status_Nm', 'Group_Previously_Completed', 'DLIC_B2B_FILE_PATTERN',
                  'DLIC_DLK_FILE_DATA_TYPE']
new_flag_info_df.drop(labels=labels_to_drop, axis=1, inplace=True)
new_flag_info_df.drop_duplicates(inplace=True)
previous_flag_info_df.drop(labels=labels_to_drop, axis=1, inplace=True)
previous_flag_info_df.drop_duplicates(inplace=True)

process_end_time = datetime.datetime.now()
print(f'Process 4 Time: {process_end_time - process_start_time}')

# ################### Determine Headers to Merge on and Create Merge DF ################### #
process_start_time = datetime.datetime.now()

merge_df = new_flag_info_df.merge(previous_flag_info_df, how='outer', on='Unique_Flag_ID', suffixes=('_new', '_prev'),
                                  indicator=False)
merge_df.drop_duplicates(inplace=True)
merge_df['DLFG_FILE_GROUP_COUNT'] = merge_df['DLFG_FILE_GROUP_COUNT_new']
merge_df['Group_or_File_Level'] = 'File'

process_end_time = datetime.datetime.now()
print(f'Process 5 Time: {process_end_time - process_start_time}')


# ################### Comparison Codes ################### #
def old_issue_int(data_frame):
    match data_frame['Group_Status_prev']:
        case 'Issue Type 1':
            return 1
        case 'Issue Type 2':
            return 2
        case 'Issue Type 3':
            return 3
        case 'Issue Type 4':
            return 4
        case 'Issue Type 5':
            return 5
        case _:
            return 0


def new_issue_int(data_frame):
    match data_frame['Group_Status_new']:
        case 'Issue Type 1':
            return 1
        case 'Issue Type 2':
            return 2
        case 'Issue Type 3':
            return 3
        case 'Issue Type 4':
            return 4
        case 'Issue Type 5':
            return 5
        case _:
            return 0


def flag_create_dt(data_frame):
    if data_frame['Unique_Flag_ID'] == 'None':
        return 'None'
    elif data_frame['Old_Issue_Int'] == 0 and data_frame['New_Issue_Int'] > 0:
        return report_date
    else:
        return data_frame['Flag_Create_Date']


def flag_close_date(data_frame):
    if data_frame['Unique_Flag_ID'] == 'None':
        return 'None'
    elif data_frame['New_Issue_Int'] == 0 and data_frame['Old_Issue_Int'] > 0:
        return report_date
    else:
        return 'None'


def date_difference(data_frame):
    if data_frame['Flag_Create_Date'] != 'None':
        start_date = datetime.datetime.strptime(data_frame['Flag_Create_Date'], '%m/%d/%Y')
        current_date = datetime.datetime.strptime(report_date, '%m/%d/%Y')

        max_total_time = abs((current_date - start_date).days) + 1
        listed_total_time = data_frame['Total_Days']

        date_diff = max_total_time - listed_total_time

        if str(date_diff) == 'nan':
            date_diff = 0

        return int(date_diff)
    else:
        return 0


def days_in_issue_type_1(data_frame):
    if data_frame['Unique_Flag_ID'] == 'None':
        return 0
    elif data_frame['New_Issue_Int'] == 1:
        if data_frame['Flag_Create_Date'] == report_date:
            return 1
        else:
            return data_frame['Days_In_Issue_Type_1'] + data_frame['Date_Difference']
    elif data_frame['Flag_Create_Date'] == report_date:
        return 0
    elif data_frame['Flag_Close_Date'] == report_date and data_frame['Old_Issue_Int'] == 1:
        return data_frame['Days_In_Issue_Type_1'] + data_frame['Date_Difference'] - 1
    else:
        return data_frame['Days_In_Issue_Type_1']


def days_in_issue_type_2(data_frame):
    if data_frame['Unique_Flag_ID'] == 'None':
        return 0
    elif data_frame['New_Issue_Int'] == 2:
        if data_frame['Flag_Create_Date'] == report_date:
            return 1
        else:
            return data_frame['Days_In_Issue_Type_2'] + data_frame['Date_Difference']
    elif data_frame['Flag_Create_Date'] == report_date:
        return 0
    elif data_frame['Flag_Close_Date'] == report_date and data_frame['Old_Issue_Int'] == 2:
        return data_frame['Days_In_Issue_Type_2'] + data_frame['Date_Difference'] - 1
    else:
        return data_frame['Days_In_Issue_Type_2']


def days_in_issue_type_3(data_frame):
    if data_frame['Unique_Flag_ID'] == 'None':
        return 0
    elif data_frame['New_Issue_Int'] == 3:
        if data_frame['Flag_Create_Date'] == report_date:
            return 1
        else:
            return data_frame['Days_In_Issue_Type_3'] + data_frame['Date_Difference']
    elif data_frame['Flag_Create_Date'] == report_date:
        return 0
    elif data_frame['Flag_Close_Date'] == report_date and data_frame['Old_Issue_Int'] == 3:
        return data_frame['Days_In_Issue_Type_3'] + data_frame['Date_Difference'] - 1
    else:
        return data_frame['Days_In_Issue_Type_3']


def days_in_issue_type_4(data_frame):
    if data_frame['Unique_Flag_ID'] == 'None':
        return 0
    elif data_frame['New_Issue_Int'] == 4:
        if data_frame['Flag_Create_Date'] == report_date:
            return 1
        else:
            return data_frame['Days_In_Issue_Type_4'] + data_frame['Date_Difference']
    elif data_frame['Flag_Create_Date'] == report_date:
        return 0
    elif data_frame['Flag_Close_Date'] == report_date and data_frame['Old_Issue_Int'] == 4:
        return data_frame['Days_In_Issue_Type_4'] + data_frame['Date_Difference'] - 1
    else:
        return data_frame['Days_In_Issue_Type_4']


def days_in_issue_type_5(data_frame):
    if data_frame['Unique_Flag_ID'] == 'None':
        return 0
    elif data_frame['New_Issue_Int'] == 5:
        if data_frame['Flag_Create_Date'] == report_date:
            return 1
        else:
            return data_frame['Days_In_Issue_Type_5'] + data_frame['Date_Difference']
    elif data_frame['Flag_Create_Date'] == report_date:
        return 0
    elif data_frame['Flag_Close_Date'] == report_date and data_frame['Old_Issue_Int'] == 5:
        return data_frame['Days_In_Issue_Type_5'] + data_frame['Date_Difference'] - 1
    else:
        return data_frame['Days_In_Issue_Type_5']


def ticket_created(data_frame):
    if data_frame['Unique_Flag_ID'] == 'None' and data_frame['Ticket_Created'] is False:
        return "FALSE"
    elif data_frame['Ticket_Created'] is True:
        return "TRUE"
    else:
        match data_frame['New_Issue_Int']:
            case 0:
                return "FALSE"
            case 1:
                if data_frame['Total_Days'] >= 7:
                    return "TRUE"
                else:
                    return "FALSE"
            case 2:
                if data_frame['Total_Days'] >= 6:
                    return "TRUE"
                else:
                    return "FALSE"
            case 3:
                if data_frame['Total_Days'] >= 5:
                    return "TRUE"
                else:
                    return "FALSE"
            case 4:
                if data_frame['Total_Days'] >= 1:
                    return "TRUE"
                else:
                    return "FALSE"
            case 5:
                return "FALSE"


def flag_notes(data_frame):
    old_flag_notes = data_frame['Flag_Notes']
    if data_frame['Group_Status_new'] == 'Good':
        return "None"
    elif data_frame['Flag_Create_Date'] == report_date:
        if data_frame['Ticket_Created'] == "TRUE":
            note_info = " , Ticket Created"
        else:
            note_info = ""
        return str(report_date) + " New Flag - " + str(data_frame['Group_Status_new']) + note_info
    elif data_frame['Flag_Close_Date'] != report_date:
        new_issue = data_frame['New_Issue_Int']
        old_issue = data_frame['Old_Issue_Int']

        if (new_issue == 1 and data_frame['Total_Days'] == 7) or (
                new_issue == 2 and data_frame['Total_Days'] == 6) \
                or (new_issue == 3 and data_frame['Total_Days'] == 5) \
                or (new_issue == 4 and data_frame['Total_Days'] == 1):
            ticket_creation = "Ticket Created"
        else:
            ticket_creation = ""

        if old_issue > new_issue:
            issue_escalation = "Issue Deescalated to Issue Type " + str(data_frame['New_Issue_Int'])
        elif old_issue < new_issue:
            issue_escalation = "Issue Escalated to Issue Type " + str(data_frame['New_Issue_Int'])
        else:
            issue_escalation = ""

        if str(data_frame['EVENT_ID_prev']) != str(data_frame['EVENT_ID_new']):
            new_files = "New File Arrived"
        else:
            new_files = ""

        if ticket_creation != "" or issue_escalation != "" or new_files != "":
            strings = [ticket_creation, issue_escalation, new_files]
            note_info = ', '.join(filter(None, strings))
            return old_flag_notes + " | " + str(report_date) + " " + note_info
        else:
            return old_flag_notes
    else:
        return old_flag_notes + " | " + str(report_date) + ' Flag Closed'


# ######## Functions for Group History ######### #
def flag_open(data_frame):
    if data_frame['Flag_Close_Date'] == 'None':
        return 'True'
    else:
        return 'False'


def flag_close_blank(data_frame):
    if data_frame['Flag_Close_Date'] == 'None':
        return ''
    else:
        return data_frame['Flag_Close_Date']


def group_flag_id(data_frame):
    group_id = str(data_frame['Group_Flag_ID'])
    group_id = group_id.split(" | B2B Profile", 1)[0]
    return group_id


def new_files_note(data_frame):
    note = data_frame['Note']
    size = data_frame['size']

    if note == 'New File Arrived' and size > 1:
        note = str(size) + " New Files Arrived"

    return note


def note_date_update(data_frame):
    date_note = str(data_frame['Date'])
    date_note = date_note + ":"
    return date_note


def flag_notes_updated(data_frame):  # Used for both Unique and Group Flags
    note = data_frame['Flag_Notes']
    if note != 'None':
        initial_list = note.split(" | ")
        new_list = []
        for item in initial_list:
            if ", " in item:
                date, note = item.split(" ", 1)
                note_list = note.split(", ")
                for x in note_list:
                    new_list.append([date, x])
            else:
                new_list.append(item.split(" ", 1))

        column_names = ["Date", "Note"]
        def_data_frame = pd.DataFrame(new_list, columns=column_names)
        def_data_frame = def_data_frame.groupby(def_data_frame.columns.tolist(), as_index=False).size()

        if data_frame['DLFG_FILE_GROUP_COUNT'] > 1:
            def_data_frame['Note'] = def_data_frame.apply(new_files_note, axis=1)

        def_data_frame.sort_values(by=["Note"], inplace=True)
        def_data_frame['Note'] = def_data_frame.groupby('Date')['Note'].transform(lambda z: ', '.join(z))
        def_data_frame.drop_duplicates(subset=column_names, keep='last', inplace=True)
        def_data_frame['Date'] = pd.to_datetime(def_data_frame['Date'], format="%m/%d/%Y").dt.strftime("%m/%d/%Y")
        def_data_frame.sort_values(by="Date", inplace=True)

        if data_frame['Group_or_File_Level'] == 'Group':
            def_data_frame['Date'] = def_data_frame.apply(note_date_update, axis=1)

        def_data_frame.drop(def_data_frame.columns.difference(column_names), axis=1, inplace=True)
        def_data_frame = def_data_frame[column_names]

        note_string = def_data_frame.to_string(index=False, header=False)
        note_string = note_string.replace("\n", " | ").strip()
        note_string = " ".join(note_string.split())
        return note_string
    else:
        return note


# ######################## Apply Functions and Update Calculated Columns ###################################### #
process_start_time = datetime.datetime.now()

merge_df['Old_Issue_Int'] = merge_df.apply(old_issue_int, axis=1)
merge_df['New_Issue_Int'] = merge_df.apply(new_issue_int, axis=1)
merge_df['Flag_Create_Date'] = merge_df.apply(flag_create_dt, axis=1)
merge_df['Flag_Close_Date'] = merge_df.apply(flag_close_date, axis=1)
merge_df['Date_Difference'] = merge_df.apply(date_difference, axis=1)
merge_df['Days_In_Issue_Type_1'] = merge_df.apply(days_in_issue_type_1, axis=1).astype('Int64')
merge_df['Days_In_Issue_Type_2'] = merge_df.apply(days_in_issue_type_2, axis=1).astype('Int64')
merge_df['Days_In_Issue_Type_3'] = merge_df.apply(days_in_issue_type_3, axis=1).astype('Int64')
merge_df['Days_In_Issue_Type_4'] = merge_df.apply(days_in_issue_type_4, axis=1).astype('Int64')
merge_df['Days_In_Issue_Type_5'] = merge_df.apply(days_in_issue_type_5, axis=1).astype('Int64')
merge_df['Total_Days'] = merge_df['Days_In_Issue_Type_1'] + merge_df['Days_In_Issue_Type_2'] + \
                         merge_df['Days_In_Issue_Type_3'] + merge_df['Days_In_Issue_Type_4'] + \
                         merge_df['Days_In_Issue_Type_5']
merge_df['Ticket_Created'] = merge_df.apply(ticket_created, axis=1)
merge_df['Flag_Notes'] = merge_df['Flag_Notes'].astype(str)
merge_df['Flag_Notes'] = merge_df.apply(flag_notes, axis=1)
merge_df['Flag_Notes'] = merge_df['Flag_Notes'].astype(str)
merge_df['Flag_Notes'] = merge_df.apply(flag_notes_updated, axis=1)  # Removes repeated notes, mostly in the case of
# multiple runs of the file
print('Functions Complete')

process_end_time = datetime.datetime.now()
print(f'Process 6 Time: {process_end_time - process_start_time}')

# ######################## Move calculated fields to new DF ######################## #
process_start_time = datetime.datetime.now()

labels = ["Unique_Flag_ID", "Flag_Create_Date", "Flag_Close_Date", "Days_In_Issue_Type_1", "Days_In_Issue_Type_2",
          "Days_In_Issue_Type_3", "Days_In_Issue_Type_4", "Days_In_Issue_Type_5", "Total_Days", "Ticket_Created",
          "Flag_Notes"]

merge_df.drop(merge_df.columns.difference(labels), axis=1, inplace=True)
merge_df = merge_df[labels]

process_end_time = datetime.datetime.now()
print(f'Process 7 Time: {process_end_time - process_start_time}')

# ############ Left Join calculated results back into Current File Info DF and Remove Good Status Types ############ #
process_start_time = datetime.datetime.now()

current_file_info_df = current_file_info_df.merge(merge_df, how='left', on='Unique_Flag_ID')
current_file_info_df.drop(current_file_info_df[current_file_info_df['Group_Status'] == 'Good'].index, inplace=True)
current_file_info_df.reset_index(drop=True, inplace=True)

process_end_time = datetime.datetime.now()
print(f'Process 8 Time: {process_end_time - process_start_time}')

# ############ Left Join New results to Previous Flag Info after dropping old results ############ #
process_start_time = datetime.datetime.now()

labels.remove('Unique_Flag_ID')  # Keep Unique Flag ID but Drop all other Calc Fields #
previous_flag_info_full_df.drop(labels=labels, axis=1, inplace=True)
previous_flag_info_full_df = previous_flag_info_full_df.merge(merge_df, how='left', on='Unique_Flag_ID')
previous_flag_info_full_df.reset_index(drop=True, inplace=True)

# Export for Use
current_file_info_df.to_csv(full_path + flag_file_name)
previous_flag_info_full_df.to_csv(previous_files_full_path + flag_file_name)
print('Files Complete')

process_end_time = datetime.datetime.now()
print(f'Process 9 Time: {process_end_time - process_start_time}')

# ################### Load Full History File ################### #
process_start_time = datetime.datetime.now()

history_df = pd.read_csv(path + full_history_file_name, index_col=0)
if history_df[history_df['Flag_Create_Date'] == report_date].shape[0] > 0:
    history_df = history_df[history_df['Flag_Create_Date'] != report_date]

# #### Create History DF #### #
history_column_labels = ['DLFG_REC_INSERT_TIME', 'DLFG_ENTERPRISE_PARTNER_NAME', 'DLFG_GROUP_NAME', 'DLGC_GROUPING_ID',
                         'DLFG_GROUP_LOADPROCESS', 'DLFC_GROUP_FREQUENCY', 'DLFG_FILE_GROUP_COUNT',
                         'DLFX_B2B_FILE_PROFILE_NAME', 'EVENT_ID', 'B2B_PARTNER_NAME', 'PROFILE_NAME',
                         'PARENT_PARTNER_NAME', 'PARENT_PARTNER_ABBR', 'PARENT_PARTNER_CODE', 'CHILD_PARTNER_NAME',
                         'CHILD_PARTNER_ABBR', 'CHILD_PARTNER_CODE', 'FILE_REC_IN_DTM', 'DEST_FILE_NM',
                         'Group_File_Status_Nm', 'Group_SLA_Status_Nm', 'Group_Status', 'Unique_Flag_ID',
                         'Flag_Create_Date', 'Flag_Close_Date', 'Days_In_Issue_Type_1', 'Days_In_Issue_Type_2',
                         'Days_In_Issue_Type_3', 'Days_In_Issue_Type_4', 'Days_In_Issue_Type_5', 'Total_Days',
                         'Ticket_Created', 'Flag_Notes']

process_end_time = datetime.datetime.now()
print(f'Process 10 Time: {process_end_time - process_start_time}')

# ################### Load Flag File ################### #
process_start_time = datetime.datetime.now()

# Previous Day's Data
previous_flag_info_full_df.drop(previous_flag_info_full_df.columns.difference(history_column_labels), axis=1,
                                inplace=True)
previous_flag_info_full_df = previous_flag_info_full_df[history_column_labels]
# flag_history_slice_df = previous_flag_info_full_df[history_column_labels]
previous_flag_info_full_df.drop_duplicates(inplace=True)
history_df = pd.concat([history_df, previous_flag_info_full_df])
history_df.sort_values(by=['Flag_Create_Date', 'Unique_Flag_ID', 'Total_Days'], ascending=True, inplace=True)
#    Added the above line in to hopefully fix some 'open' items that are closed
history_df.drop_duplicates(subset=['Unique_Flag_ID', 'Flag_Create_Date', 'Flag_Notes'], keep='last', inplace=True)

# Current Day's Data
current_file_info_df.drop(current_file_info_df.columns.difference(history_column_labels), axis=1, inplace=True)
current_file_info_df = current_file_info_df[history_column_labels]
# flag_history_slice_df = current_file_info_df[history_column_labels]
current_file_info_df.drop_duplicates(inplace=True)
history_df = pd.concat([history_df, current_file_info_df])
history_df.sort_values(by=['Flag_Create_Date', 'Unique_Flag_ID', 'Total_Days'], ascending=True, inplace=True)
#     Added the above line in to hopefully fix some 'open' items that are closed
history_df.drop_duplicates(subset=['Unique_Flag_ID', 'Flag_Create_Date', 'Flag_Notes'], keep='last', inplace=True)

process_end_time = datetime.datetime.now()
print(f'Process 11 Time: {process_end_time - process_start_time}')

# ######################################## GROUP LEVEL SUMMARY ###################################################### #
process_start_time = datetime.datetime.now()

group_column_labels = ['DLGC_GROUPING_ID', 'Client_Carrier', 'DLFC_GROUP_FREQUENCY', 'PARENT_PARTNER_ABBR',
                       'PARENT_PARTNER_CODE', 'CHILD_PARTNER_ABBR', 'CHILD_PARTNER_CODE',
                       'DLFG_ENTERPRISE_PARTNER_NAME', 'DLFG_FILE_GROUP_COUNT', 'DLFG_GROUP_NAME', 'Group_Status',
                       'DLFG_GROUP_LOADPROCESS', 'Unique_Flag_ID', 'Flag_Open', 'Flag_Create_Date', 'Flag_Close_Date',
                       'Days_In_Issue_Type_1', 'Days_In_Issue_Type_2', 'Days_In_Issue_Type_3', 'Days_In_Issue_Type_4',
                       'Days_In_Issue_Type_5', 'Total_Days', 'Ticket_Created', 'Flag_Notes']

new_group_column_labels = ['Group ID', 'Client/Carrier', 'Group Frequency', 'Parent Partner Acronym',
                           'Parent Partner Code', 'Child Partner Acronym', 'Child Partner Code',
                           'Data Lake Partner Name', 'File Count', 'Data Lake Group Name', 'Group Status', 'File Type',
                           'Group Flag ID', 'Flag Open', 'Flag Create Date', 'Flag Close Date', 'Days In Issue Type 1',
                           'Days In Issue Type 2', 'Days In Issue Type 3', 'Days In Issue Type 4',
                           'Days In Issue Type 5', 'Total Days', 'Ticket Created', 'Flag Notes']

# ######## Revamp Latest History DF ############ #
history_df['DLFG_GROUP_LOADPROCESS'] = history_df['DLFG_GROUP_LOADPROCESS'].astype(str)
history_df['DLFG_GROUP_NAME'] = history_df['DLFG_GROUP_NAME'].astype(str)
history_df['Client_Carrier'] = history_df.apply(client_carrier, axis=1)
history_df['DLFC_GROUP_FREQUENCY'] = history_df['DLFC_GROUP_FREQUENCY'].astype(str)
history_df['DLFC_GROUP_FREQUENCY'] = history_df.apply(frequency_update, axis=1)

process_end_time = datetime.datetime.now()
print(f'Process 12 Time: {process_end_time - process_start_time}')

# Full History File
process_start_time = datetime.datetime.now()

print(f'History Dataframe Shape:{history_df.shape}')
history_df.to_csv(path + full_history_file_name)

process_end_time = datetime.datetime.now()
print(f'Process 13 Time: {process_end_time - process_start_time}')

# ########### Create Group Level History ################# #
process_start_time = datetime.datetime.now()

history_df['Flag_Open'] = history_df.apply(flag_open, axis=1)
history_df['Flag_Close_Date'] = history_df.apply(flag_close_blank, axis=1)
history_df.drop_duplicates(subset=['Unique_Flag_ID'], keep='last', inplace=True)
history_df.drop(history_df.columns.difference(group_column_labels), axis=1, inplace=True)
history_df = history_df[group_column_labels]
history_df.rename(columns={'Unique_Flag_ID': 'Group_Flag_ID'}, inplace=True)
history_df['Group_Flag_ID'] = history_df.apply(group_flag_id, axis=1)
history_df['Group_or_File_Level'] = 'Group'

process_end_time = datetime.datetime.now()
print(f'Process 14 Time: {process_end_time - process_start_time}')

# ########## Move File Level to Group Level ############## #
process_start_time = datetime.datetime.now()

history_df.drop_duplicates(keep='last', inplace=True)
history_df['Flag_Notes'].astype(str)
history_df['Flag_Notes'] = history_df.groupby('Group_Flag_ID')['Flag_Notes'].transform(lambda x: ' | '.join(x))
history_df.drop_duplicates(keep='last', inplace=True)
history_df['Flag_Notes'] = history_df.apply(flag_notes_updated, axis=1)
history_df.drop_duplicates(subset=['Group_Flag_ID'], keep='last', inplace=True)
history_df.reset_index(drop=True, inplace=True)
history_df.drop(['Group_or_File_Level'], axis=1, inplace=True)

process_end_time = datetime.datetime.now()
print(f'Process 15 Time: {process_end_time - process_start_time}')

# Rename Columns
process_start_time = datetime.datetime.now()

history_df.columns = new_group_column_labels

# Load Table
group_history_worksheet = group_trending_workbook.sheets[group_history_sheet_name]
print("Sheet Found")
group_history_worksheet.tables[group_history_table_name].update(history_df, index=False)
print('Table Updated')
group_trending_workbook.save()
print('Workbook Saved')
group_trending_workbook.close()
print('Workbook Closed')

process_end_time = datetime.datetime.now()
print(f'Process 16 Time: {process_end_time - process_start_time}')

# ################### Show the ending date/time ################### #
end_time = datetime.datetime.now()
dt_string = end_time.strftime("%m/%d/%Y %H:%M:%S.%f")
print(f"\nFinish: {dt_string}\nTotal Time: {end_time - start_time}")
