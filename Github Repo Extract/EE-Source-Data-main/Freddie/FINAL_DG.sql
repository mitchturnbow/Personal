--Update Projects Table with 850 Loads Info
DELETE FROM DL_DL_OPS_MET.PROJECTS;
INSERT INTO DL_DL_OPS_MET.PROJECTS
SELECT *
FROM EDW_ELG_FL.ARTFUPR
WHERE (CONTEXT_CD like 'FUSYLD%' or context_cd like 'FUSMFS%' or context_cd like 'FUSSPL%') and 
description_txt like '%DATA_GAP%' and (comment_txt like '%EMPLOY%' or comment_txt like '%SPLAT%')
and PRJST_RF = 'OPEN' AND cast(START_DTM as date) >= '2022-10-15';

--Update Data Gaps Verifications Table to reference ultimate verification result of policies
DELETE FROM DL_DL_OPS_MET.DG_VERIFICATION;
INSERT INTO DL_DL_OPS_MET.DG_VERIFICATION
SELECT '1' as ROW_ID,P.CONTEXT_CD, P.PROJECT_ID,V.CLIENT_CD, V.COVERAGE_ID, V.LOAD_DT,
V.CARRIER_CD, V.CARRIER_OFFICE_CD, 
CASE WHEN V.VERST_RF = 'QUEUED' then MV.LAST_VERST_RF else V.VERST_RF end as VERST_RF
FROM DL_DL_OPS_MET.PROJECTS as P LEFT JOIN EDW_AR_FL.ARTCOVV as V
ON P.CONTEXT_CD = V.CONTEXT_CD and P.PROJECT_ID = V.PROJECT_ID
LEFT JOIN EDW_AR_FL.ARTFUVR as MV ON MV.COVERAGE_ID = V.COVERAGE_ID and 
V.CONTEXT_CD = MV.CONTEXT_CD;

--Update Data Gap Records Table containing info on ELGVER,MCHINVLD, Updated Polnums, Etc
DELETE FROM DL_DL_OPS_MET.DG_RECORDS;
INSERT INTO DL_DL_OPS_MET.DG_RECORDS
SELECT F.CLIENT_CD, F.CONTEXT_CD, F.PROJECT_ID, F.RECIP_MA_NUM, F.RECIP_FIRST_NM, F.RECIP_LAST_NM, 
F.RECIP_DOB_DT, F.RECIP_SSN_NUM, F.CARRIER_CD, F.POLICY_NUM, F.PLCTP_RF, F.POLICY_START_DT, 
F.POLICY_END_DT, F.VERST_RF, F.VRFRV_RF, F.VRFRV_CMT, F.VERIFY_DT, F.VRFRV_DT, F.REPORT_DT,
F.NEW_CONTEXT_CD, F.EMP_NM,F.POSTING_ID, F.COVERAGE_ID
FROM DL_DL_OPS_MET.PROJECTS as P left join EDW_ELG_FL.ARTFUSP as F
ON P.CONTEXT_CD = F.CONTEXT_CD and P.PROJECT_ID = F.PROJECT_ID
WHERE P.CONTEXT_CD IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY F.CLIENT_CD, F.CONTEXT_CD, F.POSTING_ID ORDER BY F.CLIENT_CD, F.CONTEXT_CD, F.POSTING_ID) = 1;

--Update V9 Source Table to track all policies on NEDB with primary source of V9
DELETE FROM DL_DL_OPS_MET.V9_SOURCE;
INSERT INTO DL_DL_OPS_MET.V9_SOURCE
SELECT	SEGMENT_ID, SEGMENT_TYPE_CD, CARRIER_CD, CARRIER_OFFICE_CD,
GROUP_NUM, POLICY_NUM, INSURED_LAST_NM, INSURED_FIRST_NM, INSURED_MIDDLE_INIT_NM,
INSURED_SEX_CD, INSURED_SSN_NUM, INSURED_BIRTH_DT, INSURED_STREET_ADDRESS_TXT,
INSURED_CITY_TXT, INSURED_STATE_CD, INSURED_ZIP_CD, PATIENT_RELATION_TO_INS_CD,
DEPENDENT_LAST_NM, DEPENDENT_FIRST_NM, DEPENDENT_BIRTH_DT, DEPENDENT_SEX_CD,
DEPENDENT_SSN_NUM, EMPLOYER_NM, HOSP_ELIG_START_DT,
HOSP_ELIG_STOP_DT, OUTPAT_ELIG_START_DT, OUTPAT_ELIG_STOP_DT,
MED_ELIG_START_DT, MED_ELIG_STOP_DT, PHAR_ELIG_START_DT, PHAR_ELIG_STOP_DT,
DENTAL_ELIG_START_DT, DENTAL_ELIG_STOP_DT, VISION_ELIG_START_DT,
VISION_ELIG_STOP_DT,ORIGINAL_SOURCE_FILE_DT, ORIG_SOURCE_CODE_CREATE_CD,
CREATE_DT,POLICY_TYPE, HOSP_BENEFIT_FLAG, OUTPAT_BENEFIT_FLAG,
MED_BENEFIT_FLAG, PHAR_BENEFIT_FLAG, DENTAL_BENEFIT_FLAG, VISION_BENEFIT_FLAG
FROM	EDW_ELG_FL.NED_525
WHERE ORIG_SOURCE_CODE_CREATE_CD = 'V9' AND LAST_SOURCE_FILE_DT >= '2022-11-01';

--Update Data Gaps Matches Table to track all policies that ultimately match and land on ELGF
DELETE FROM DL_DL_OPS_MET.DG_MATCHES;
insert into dl_dl_ops_met.DG_MATCHES
SELECT	'1' as ROW_ID,V.SEGMENT_ID, V.SEGMENT_TYPE_CD, V.CARRIER_CD, V.CARRIER_OFFICE_CD,
V.GROUP_NUM, V.POLICY_NUM, V.INSURED_LAST_NM, V.INSURED_FIRST_NM, V.INSURED_MIDDLE_INIT_NM,
V.INSURED_SEX_CD, V.INSURED_SSN_NUM, V.INSURED_BIRTH_DT, V.INSURED_STREET_ADDRESS_TXT,
V.INSURED_CITY_TXT, V.INSURED_STATE_CD, V.INSURED_ZIP_CD, V.PATIENT_RELATION_TO_INS_CD,
V.DEPENDENT_LAST_NM, V.DEPENDENT_FIRST_NM, V.DEPENDENT_BIRTH_DT, V.DEPENDENT_SEX_CD,
V.DEPENDENT_SSN_NUM, V.EMPLOYER_NM, V.HOSP_ELIG_START_DT,
V.HOSP_ELIG_STOP_DT, V.OUTPAT_ELIG_START_DT, V.OUTPAT_ELIG_STOP_DT,
V.MED_ELIG_START_DT, V.MED_ELIG_STOP_DT, V.PHAR_ELIG_START_DT, V.PHAR_ELIG_STOP_DT,
V.DENTAL_ELIG_START_DT, V.DENTAL_ELIG_STOP_DT, V.VISION_ELIG_START_DT,
V.VISION_ELIG_STOP_DT,ORIGINAL_SOURCE_FILE_DT, V.ORIG_SOURCE_CODE_CREATE_CD,
V.CREATE_DT,POLICY_TYPE, V.HOSP_BENEFIT_FLAG, V.OUTPAT_BENEFIT_FLAG,
V.MED_BENEFIT_FLAG, V.PHAR_BENEFIT_FLAG, V.DENTAL_BENEFIT_FLAG, V.VISION_BENEFIT_FLAG, e.client_Cd, e.ri_ma_num,e.elgf_id
FROM DL_DL_OPS_MET.V9_SOURCE AS V LEFT JOIN EDW_ELG_FL.ARTELGF AS E
ON E.SEGMENT_ID = V.SEGMENT_ID;

--Update Data Gaps Billings Table to track billings, transact status and reason codes
DELETE FROM DL_DL_OPS_MET.DG_BILLINGS;
INSERT INTO DL_DL_OPS_MET.DG_BILLINGS
SELECT M.SEGMENT_ID, M.ELGF_ID, M.CLIENT_CD, AR.MA_NUM, AR.ICN_NUM,AR.AR_SEQ_NUM,AR.TRANSACT_STATUS_CD,AR.CARRIER_ACTION_CD,D.LITERAL_MEANING,
AR.BILL_DT,AR.FROM_DOS_DT, AR.THRU_DOS_DT, AR.MA_PAID_AMT, AR.CLAIM_TYPE_CD, AR.MA_PAID_DT, AR.CARRIER_CD, AR.CERT_NUM,
AR.ELIGIBLE_START_DT, AR.ELIGIBLE_END_DT, AR.INSURED_FIRST_NM,AR.INSURED_LAST_NM,AR.FIRST_NM, AR.INSURED_SSN_NUM, AR.LAST_NM,
AR.RELATE_CD,AR.SSN_NUM, AR.REMIT_DT as CLAIM_REMIT_DT, AR.REMIT_AMT as CLAIM_REMIT_AMT
FROM DL_DL_OPS_MET.DG_MATCHES AS M LEFT JOIN EDW_AR_FL.ARTBASE AS AR
ON M.RI_MA_NUM = AR.MA_NUM AND AR.BILL_DT > M.CREATE_DT AND AR.PROJECT_CD >= '31'
AND AR.CLAIM_TYPE_CD NOT IN ('10','11','12','18')
AND (M.CLIENT_CD = AR.CONTRACT_NUM OR M.CLIENT_CD = AR.CHILD_CONTRACT_NUM)
AND ORIG_SOURCE_CODE_CREATE_CD = 'V9' AND M.CARRIER_CD = AR.CARRIER_CD
LEFT JOIN (
SELECT CARRIER_ACTION_CD, LITERAL_MEANING, DENIAL_CATEGORY, DENIAL_SUBCATEGORY
FROM EDW_AR_FL.DENIAL_DICTIONARY_ALL
WHERE LITERAL_MEANING <> ''
qualify row_number() over (partition by carrier_action_cd order by carrier_action_cd, literal_meaning desc, DW_INSERT_TIMESTAMP desc) = 1
) as d
on d.carrier_action_cd = ar.carrier_action_cd
where AR.MA_NUM is not null;

--Update Final Data Gaps Report
DELETE FROM DL_DL_OPS_MET.FINAL_DG;
INSERT INTO DL_DL_OPS_MET.FINAL_DG
 SELECT 
        	--Pull in additional information for each life from AS DG_RECORDS table
F.RECIP_FIRST_NM AS DG_RECORDS_FIRST_NM,
F.RECIP_LAST_NM AS DG_RECORDS_LAST_NM,
F.RECIP_SSN_NUM AS DG_RECORDS_SSN,
F.POLICY_NUM AS POLICY_NUM,
F.RECIP_MA_NUM AS DG_MA_NUM,
F.POLICY_START_DT AS DG_POLICY_START,
F.POLICY_END_DT AS DG_POLICY_END,
 --Pull in additional information for each life from AS DG_MATCHES table
E.POLICY_NUM AS DG_MATCHES_POLICY,
E.INSURED_SSN_NUM AS DG_MATCHES_SSN,
E.INSURED_FIRST_NM AS DG_MATCHES_FIRST_NM,
E.INSURED_LAST_NM AS DG_MATCHES_LAST_NM,
 --Project Columns
P.CONTEXT_CD,
P.PROJECT_ID,
CAST (P.START_DTM AS DATE) AS RUN_DT,
 --Verification Columns
V.CLIENT_CD,
V.COVERAGE_ID,
V.LOAD_DT,
V.CARRIER_CD,
V.VERST_RF AS VERIFICATION_STATUS,
 --FUSP Columns
F.VERST_RF AS VERIFICATION_RESULT,
F.POSTING_ID,
F.VERIFY_DT,
F.VRFRV_RF,
F.VRFRV_CMT,
F.VRFRV_DT,
F.REPORT_DT,
 --NEDB Columns
N.SEGMENT_ID,
N.SEGMENT_TYPE_CD,
N.ORIGINAL_SOURCE_FILE_DT,
N.ORIG_SOURCE_CODE_CREATE_CD,
 --ELGF Columns
E.ELGF_ID,
 --AR Columns
B.RECOVERED_AMT,
 --Verifications Detail Fields

CASE
    WHEN V.CLIENT_CD IS NOT NULL AND V.VERST_RF IN  ('NOLOAD','COVRXWALK') THEN 'REJECT' ELSE 'PASSED'
END AS VERIF_ROUTING_CHECK,
    
CASE
    WHEN V.CLIENT_CD IS NOT NULL AND V.VERST_RF IN  ('ETR_QUEUED','WEB_NEWLD','UNASGND','FLATQUEUED') THEN 'PENDING' ELSE 'PROCESSED'
END AS VERIF_PENDING_CHECK,
    
CASE
    WHEN V.CLIENT_CD IS NOT NULL AND V.VERST_RF IN  ('DUPLICATES') THEN 'DUPLICATE' ELSE 'UNIQUE'
END AS VERIF_DUPE_CHECK,
    
CASE
    WHEN V.CLIENT_CD IS NOT NULL AND V.VERST_RF IN  ('ONHOLD','ECAREONHLD','CARACCISUE',NULL) THEN 'ON HOLD' ELSE 'ACTIVE'
END AS VERIF_HOLD_CHECK,
    
CASE
    WHEN V.CLIENT_CD IS NOT NULL AND V.VERST_RF IN  ('MCHINVLD','BAD_DATA','MISINFO','MIS_ELGFID') THEN 'DATA ERROR' ELSE 'PASSED'
END AS VERIF_DATA_CHECK,
    
CASE
    WHEN V.CLIENT_CD IS NOT NULL AND V.VERST_RF IN  ('ETR_CMPLTD','EDIQCCOMPL','FLATFILE','MATCH_NED','SKP_ECARE') THEN 'COMPLETE' ELSE 'INCOMPLETE'
END AS VERIF_COMP_CHECK,
     --FUSP Delivery Detail Fields

CASE
    WHEN F.VERST_RF IN ('ELGVER','SKP_ECARE','ELGTERM') THEN 'VALID POLICY'
    WHEN F.VERST_RF IS NULL THEN 'NO VALUE' ELSE 'INVALID POLICY'
END AS DELIV_VALID_CHECK,
    
CASE
    WHEN F.CONTEXT_CD LIKE '%MFS%' AND F.NEW_CONTEXT_CD IS NOT NULL THEN 'MOVED TO YLD'
    WHEN F.CONTEXT_CD LIKE '%SPL%' THEN 'NO MOVE NEEDED' ELSE 'NOT MOVED'
END AS DELIV_FLIP_CHECK,
    
CASE
    WHEN F.CONTEXT_CD LIKE '%SPL%' AND F.VRFRV_RF = '000' THEN 'PENDING BACKEND'
    WHEN F.YLD_CONTEXT_CD IS NOT NULL AND F.YLD_VRFRV_RF = '000' THEN 'PENDING BACKEND'
    WHEN F.VRFRV_RF = '110' OR F.YLD_VRFRV_RF = '110' THEN 'CLEARED BACKEND'
    WHEN F.CONTEXT_CD LIKE '%MFS%' AND F.YLD_CONTEXT_CD IS NULL THEN 'PENDING FLIP' ELSE 'REJECTED'
END AS DELIV_BE_CHECK,
    
CASE
    WHEN (F.VRFRV_RF = '110' AND UPPER(ltrim(rtrim(F.VRFRV_CMT))) LIKE '%DELIV%') OR (F.YLD_VRFRV_RF = '110' AND UPPER(ltrim(rtrim(F.YLD_VRFRV_CMT))) LIKE '%DELIV%') 
THEN 'DELIVERED' ELSE 'NOT DELIVERED'
END DELIV_TRANSMIT_CHECK,
     --NEDB Detail

CASE
    WHEN N.SEGMENT_ID IS NOT NULL THEN 'FUS2NED SUCCESS' ELSE 'FUS2NED PENDING'
END AS NED_SEGMENT_CHECK,
    
CASE
    WHEN N.SEGMENT_ID IS NOT NULL AND F.YLD_POSTING_ID IS NOT NULL THEN LTRIM(RTRIM(F.CLIENT_CD))||LTRIM(RTRIM(F.YLD_CONTEXT_CD))||CAST(F.YLD_POSTING_ID AS VARCHAR(16))
    WHEN N.SEGMENT_ID IS NOT NULL AND F.CONTEXT_CD LIKE '%SPL%' THEN LTRIM(RTRIM(F.CLIENT_CD))||LTRIM(RTRIM(F.CONTEXT_CD))||CAST(F.POSTING_ID AS VARCHAR(16)) ELSE 'PENDING TRACE_CD'
END AS NED_TRACE_CD,
     --ELGF Detail

CASE
    WHEN N.SEGMENT_ID IS NOT NULL AND E.ELGF_ID IS NULL THEN 'PENDING MATCH'
    WHEN N.SEGMENT_ID IS NOT NULL AND E.ELGF_ID IS NOT NULL THEN 'MATCHED' ELSE 'N/A'
END AS ELG_MATCH_CHECK,
     --Billing Detail

CASE
    WHEN B.ELGF_ID IS NULL THEN 'NO BILLINGS'
    WHEN B.ELGF_ID IS NOT NULL THEN 'CLAIMS BILLED' ELSE 'N/A'
END AS BILL_HIT_CHECK,
    
CASE
    WHEN B.ELGF_ID IS NOT NULL AND B.RECOVERED_AMT > 0 THEN 'RECOVERED' ELSE 'NO RECOVERIES'
END AS BILL_RECOV_CHECK
FROM DL_DL_OPS_MET.PROJECTS AS P LEFT JOIN DL_DL_OPS_MET.DG_VERIFICATION AS V
ON P.CONTEXT_CD = V.CONTEXT_CD AND P.PROJECT_ID = V.PROJECT_ID
LEFT JOIN DL_DL_OPS_MET.DG_RECORDS AS F
ON V.CONTEXT_CD = F.CONTEXT_CD AND V.COVERAGE_ID = F.COVERAGE_ID AND V.CLIENT_CD = F.CLIENT_CD
LEFT JOIN DL_DL_OPS_MET.V9_SOURCE AS N
ON N.CARRIER_CD = F.CARRIER_CD AND N.DEPENDENT_SSN_NUM = F.RECIP_SSN_NUM AND F.POLICY_NUM = N.POLICY_NUM
LEFT JOIN DL_DL_OPS_MET.DG_MATCHES AS E
ON N.SEGMENT_ID = E.SEGMENT_ID
LEFT JOIN (
SELECT SEGMENT_ID,
    ELGF_ID,
    CLIENT_CD,
    CARRIER_CD,
    SUM(CLAIM_REMIT_AMT) AS RECOVERED_AMT
    FROM DL_DL_OPS_MET.DG_BILLINGS
    GROUP BY 1,
        2,
        3,
        4
) AS B
ON B.ELGF_ID = E.ELGF_ID)

