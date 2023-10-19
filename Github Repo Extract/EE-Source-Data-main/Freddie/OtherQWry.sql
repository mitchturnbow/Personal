with elgr as (
select elgr.client_cd, ref.carrier_cd, ref.client_carrier_nm, elgr.policy_num, elgr.ma_num, elgr.coverage_start_dt, ph_first_nm, ph_last_nm,ph_ssn_num,ph_dob_dt
case when elgr.coverage_end_dt is null then cast('9999-12-31' as date) else elgr.coverage_end_dt end as coverage_end_dt -- Standardize open coverage, if null returns 12/31/9999
from edw_elg_fl.artelgr elgr
	left join edw_ar_fl.artref7 ref
		on elgr.client_carrier_cd = ref.client_carrier_cd
where (elgr.client_cd = '303'
and (elgr.coverage_start_dt > current_date - interval '3' year and elgr.coverage_start_dt <= current_date)
and (elgr.coverage_end_dt > current_date or elgr.coverage_end_dt is null))
and (extract(year from current_date)- extract(year from elgr.dp_dob_dt)) between 18 and 65
),

ccdb as (select client_cd, ma_num, icn_num, substr(icn_num,'3') as join_icn, claim_from_dt, sum(ma_paid_amt) as ma_paid_amt, ptnt_first_nm, ptnt_last_nm, ptnt_dob_dt
from edw_clm_fl.claim_common_1
where client_cd in ('303')
and ma_num in (select ma_num from elgr)
and claim_from_dt >= '2022-01-01'
group by 1,2,3,4,5),

elgf as (select contract_num, carrier_cd, policy_num, insured_last_nm, insured_first_nm, dependent_ssn_num,ph_dob_dt
from edw_elg_fl.artelgf
where contract_num = '303'
and carrier_cd in (select carrier_cd from elgr)
and policy_num in (select policy_num from elgr)
),

ar as (select contract_num, icn_num, from_dos_dt, sum(ma_paid_amt) as ma_paid_amt,ma_num, cert_num, first_nm,last_nm,patient_dob_dt, ssn_num
from edw_ar_fl.artbase
where contract_num in ('303')
and project_cd >='31'
and ma_num in (select ma_num from ccdb)
and icn_num in (select substr(icn_num, '3') from ccdb)
group by 1,2,3)

select elgr.client_cd, elgr.carrier_cd, elgr.client_carrier_nm, count(distinct(elgr.ma_num)) as elgr_counts, count(distinct(ccdb.icn_num)) as ccdb_claim_count, count(distinct(ar.icn_num)) as ar_claim_count
from ccdb
	left join elgr
		on ccdb.client_cd = elgr.client_cd
		and ccdb.ma_num = elgr.ma_num
	left join ar
		on ccdb.client_cd = ar.contract_num
		and ccdb.join_icn = ar.icn_num
		and ccdb.ptnt_first_nm = ar.first_nm
		and ccdb.ptnt_last_nm = ar.last_nm
	left join elgf
		on ccdb.client_cd = elgf.contract_num
		and ccdb.join_icn = elgf.policy_num
		and ccdb.ptnt_dob_dt = elgf.ph_dob_dt
		and ccdb.ptnt_first_nm = elgf.insured_first_nm
		and ccdb.ptnt_last_nm = elgf.insured_last_nm
where ccdb.claim_from_dt >= elgr.coverage_start_dt
group by 1,2,3
