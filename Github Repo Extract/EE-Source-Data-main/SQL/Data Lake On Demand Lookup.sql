-- Microsoft SQL Server Management Studio 
-- Test Server: WTDBEIM001
-- Prod Server: WPDBEIM001

use [EIM_DB]
select A.DLK_ONDEMAND_CONFIG_ID, A.DLK_ONDEMAND_PARTNER, B.DLK_SRC_DELIVERY_TARGET_NAME, A.DLK_ONDEMAND_TARGET_LOCATION, A.DLK_ONDEMAND_FILEDETAILS, A.DLK_ONDEMAND_UPDATE_DT, A.DLK_ONDEMAND_UPDATED_USER, A.DLK_ONDEMAND_JOB_STATUS
from [dbo].[Source_Delivery_OnDemand] A
join [dbo].[Source_Delivery_Servers] B
on a.DLK_ONDEMAND_SERVER_ID=b.DLK_SRC_DELIVERY_SERVER_ID
where A.DLK_ONDEMAND_UPDATED_USER ='e007997'
and a.DLK_ONDEMAND_FILEDETAILS ='\prod\hms\dlk\data\anthem_northeast\anthem_northeast\err\carrier_eligibility_ct_mbr\version%3Dv1\ENT_EDW_SDE_HMS_CT_MDCD_MBRMNTHLY_ALLPHI_20220401-20220430.out.gz####1977476707_1081186598'
