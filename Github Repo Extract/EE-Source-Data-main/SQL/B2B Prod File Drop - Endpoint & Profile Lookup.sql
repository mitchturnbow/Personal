-- Microsoft SQL Server Management Studio
-- Server: wpdbb2b003

use [INFPDX]
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
left join (select profile_id, value_
from [dbo].[DX_PROFILE_PROP_INSTANCE]
where definition_id='46010') prop
on profiles.profile_id=prop.PROFILE_ID
where --pattern.[READ_PATTERN_REGEX] like '%mdcd%mbrmnthly%o%'
profiles.profile_name in ('INBD_Anthem_Hms_Ct_Mdcd_Mbr_Allphi_Carrier_Eligibility', 'INBD_Anthem_Hms_Me_Mdcd_Mbr_Allphi_Carrier_Eligibility') 

