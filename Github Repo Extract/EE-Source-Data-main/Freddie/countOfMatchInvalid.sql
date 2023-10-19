with directBillRecords as (
	SELECT context_cd, project_id
	FROM EDW_ELG_FL.ARTFUPR
	WHERE (CONTEXT_CD like 'FUSYLD%' or context_cd like 'FUSMFS%' or context_cd like 'FUSSPL%') and 
	description_txt like '%DATA_GAP%' and (comment_txt like '%EMPLOY%' or comment_txt like '%SPLAT%')
	and PRJST_RF = 'OPEN' AND cast(START_DTM as date) >= '2022-10-15'),
lastSubmission AS (
SELECT *
FROM   dl_dl_ops_met.final_dg b
WHERE  (b.context_cd,b.project_id) in
       (SELECT context_cd,project_id
        FROM   directBillRecords)),
              
Routing_Check AS (select fin.client_cd,fin.CARRIER_CD,count((fin.VERIF_ROUTING_CHECK)) as Routing_Check
from lastSubmission as fin
where fin.VERIF_ROUTING_CHECK in ('REJECT')
group by 1,2),

VERIF_PENDING_CHECK AS (select fin.client_cd,fin.CARRIER_CD,count((fin.VERIF_PENDING_CHECK)) as VERIF_PENDING_CHECK
from lastSubmission as fin
where fin.VERIF_PENDING_CHECK in ('PENDING')
group by 1,2),

VERIF_DUPE_CHECK AS (select fin.client_cd,fin.CARRIER_CD,count((fin.VERIF_DUPE_CHECK)) as VERIF_DUPE_CHECK
from lastSubmission as fin
where fin.VERIF_DUPE_CHECK in ('DUPLICATE')
group by 1,2),

VERIF_HOLD_CHECK AS (select fin.client_cd,fin.CARRIER_CD,count((fin.VERIF_HOLD_CHECK)) as VERIF_HOLD_CHECK
from lastSubmission as fin
where fin.VERIF_HOLD_CHECK in ('ON HOLD')
group by 1,2),

VERIF_DATA_CHECK AS (select fin.client_cd,fin.CARRIER_CD,count((fin.VERIF_DATA_CHECK)) as VERIF_DATA_CHECK
from lastSubmission as fin
where fin.VERIF_DATA_CHECK in ('DATA ERROR')
group by 1,2),

VERIF_INCOMP_CHECK AS (select fin.client_cd,fin.CARRIER_CD,count((fin.VERIF_COMP_CHECK)) as VERIF_INCOMP_CHECK
from lastSubmission as fin
where fin.VERIF_COMP_CHECK in ('INCOMPLETE')
group by 1,2),

VERIF_COMP_CHECK AS (select fin.client_cd,fin.CARRIER_CD,count((fin.VERIF_COMP_CHECK)) as VERIF_COMP_CHECK
from lastSubmission as fin
where fin.VERIF_COMP_CHECK in ('COMPLETE')
group by 1,2),

INVALID_POLICY AS (select fin.client_cd,fin.CARRIER_CD,count((fin.DELIV_VALID_CHECK)) as INVALID_POLICY
from lastSubmission as fin
where fin.DELIV_VALID_CHECK in ('INVALID POLICY')
group by 1,2),

VALID_POLICY AS (select fin.client_cd,fin.CARRIER_CD,count((fin.DELIV_VALID_CHECK)) as VALID_POLICY
from lastSubmission as fin
where fin.DELIV_VALID_CHECK in ('VALID POLICY')
group by 1,2),

NO_VALUE AS (select fin.client_cd,fin.CARRIER_CD,count((fin.DELIV_VALID_CHECK)) as NO_VALUE
from lastSubmission as fin
where fin.DELIV_VALID_CHECK in ('NO VALUE')
group by 1,2),

carrierMatchPerformance as (
	SELECT 
	a.client_cd,
	a.carrier_cd,
	CASE WHEN a.Routing_Check IS NULL THEN '0' ELSE a.Routing_Check END AS ROUTING_ISSUE,
	CASE WHEN b.VERIF_PENDING_CHECK IS NULL THEN '0' ELSE b.VERIF_PENDING_CHECK END AS PENDING,
	CASE WHEN c.VERIF_DUPE_CHECK IS NULL THEN '0' ELSE c.VERIF_DUPE_CHECK END AS DUPLICATES,
	CASE WHEN d.VERIF_HOLD_CHECK IS NULL THEN '0' ELSE d.VERIF_HOLD_CHECK END AS HOLDS,
	CASE WHEN e.VERIF_DATA_CHECK IS NULL THEN '0' ELSE e.VERIF_DATA_CHECK END AS DATA_ERROR,
	f.VERIF_INCOMP_CHECK,
	CASE WHEN g.VERIF_COMP_CHECK IS NULL THEN '0' ELSE g.VERIF_COMP_CHECK END AS COMPLETED,
	CASE WHEN h.INVALID_POLICY IS NULL THEN '0' ELSE h.INVALID_POLICY END AS INVALID_POLICY_COUNT,
	CASE WHEN i.VALID_POLICY IS NULL THEN '0' ELSE i.VALID_POLICY END AS VALID_POLICY_COUNT,
	CASE WHEN j.NO_VALUE IS NULL THEN '0' ELSE j.NO_VALUE END AS NO_VALUE_COUNT,
	SUM(COMPLETED+f.VERIF_INCOMP_CHECK) AS TOTAL_RECORDS,
	ROUND(INVALID_POLICY_COUNT * 100.0 / (TOTAL_RECORDS), 1) as INVALID_POLICIES_PERCENT,
	ROUND(VALID_POLICY_COUNT * 100.0 / (TOTAL_RECORDS), 1) as VALID_POLICIES_PERCENT,
	ROUND(NO_VALUE_COUNT * 100.0 / (TOTAL_RECORDS), 1) as NO_VALUE_PERCENT,
	ROUND(COMPLETED * 100.0 / (TOTAL_RECORDS), 1) as COMPLETION_PERCENT
	from Routing_Check a
	left join VERIF_PENDING_CHECK b 
	on a.client_cd = b.client_cd
	and a.carrier_cd = b.carrier_cd
	left join VERIF_DUPE_CHECK c
	on a.client_cd = c.client_Cd
	and a.carrier_cd = c.carrier_cd
	left join VERIF_HOLD_CHECK d
	on a.client_cd = d.client_cd
	and a.carrier_cd = d.carrier_cd
	left join VERIF_DATA_CHECK e
	on a.client_cd = e.client_cd
	and a.carrier_cd = e.carrier_cd
	left join VERIF_INCOMP_CHECK f
	on a.client_cd = f.client_cd
	and a.carrier_cd = f.carrier_cd
	left join VERIF_COMP_CHECK g
	on a.client_cd = g.client_cd
	and a.carrier_cd =g.carrier_cd
	left join INVALID_POLICY h
	on a.client_cd = h.client_cd
	and a.carrier_cd = h.carrier_cd
	left join VALID_POLICY i
	on a.client_cd = i.client_cd
	and a.carrier_cd = i.carrier_cd
	left join NO_VALUE j
	on a.client_cd = j.client_cd
	and a.carrier_cd = j.carrier_cd
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
	)
/*
select *
from carrierMatchPerformance w
order by w.VALID_POLICIES_PERCENT desc
*/
	
SELECT 
SUM(B.VALID_POLICY_COUNT) AS validPolicies,
SUM(B.INVALID_POLICY_COUNT) AS invalidPolicies,
SUM(B.NO_VALUE_COUNT) AS noValue,
SUM(B.COMPLETED) as completed,
SUM(B.VERIF_INCOMP_CHECK) as incompleted,
SUM(B.ROUTING_ISSUE) as routingIssue,
SUM(B.PENDING) as pending,
SUM(B.DUPLICATES) as duplicates,
SUM(B.HOLDS) as holds,
SUM(B.DATA_ERROR) as dataError,
SUM(B.TOTAL_RECORDS) as totalRecords
FROM carrierMatchPerformance as B

