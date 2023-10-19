-- Microsoft SQL Server Management Studio
-- Server: wtdbb2b003

use [INFTDX]
select pattern.READ_PATTERN_REGEX, end_point.E_NAME, 
case end_point.ENABLED
	when '1' then 'Enabled'
	when '0' then 'Disabled'
	else '' end as ENDPOINT_ENABLED, 
end_point.FILE_PATH, profiles.PROFILE_Name, prop.VALUE_ as PROFILE_PATTERN, part.PARTNER_NAME
from [dbo].[DX_ENDPOINT_READ_PATTERN] pattern
join [dbo].[DX_ENDPOINT] end_point
on pattern.ENDPOINT_ID= end_point.ID
join [dbo].[DX_PROFILE] profiles
on pattern.PROFILE_ID=profiles.PROFILE_ID
join [dbo].[DX_PARTNER] part
on profiles.PARTNER_ID=part.PARTNER_ID
join (select profile_id, value_
from [dbo].[DX_PROFILE_PROP_INSTANCE]
where definition_id='65105') prop
on profiles.profile_id=prop.PROFILE_ID
where --pattern.[READ_PATTERN_REGEX] like '%GATEMMPCF%'
profiles.profile_name in ('INBD_New_York_TPL_Eny_Sw0021_B100_Cyc_Claim_Medical_File')
--end_point.E_NAME ='INBD_California_DHS_Receive'
