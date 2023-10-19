WITH DEDUPE 
    AS (
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
        ON B.ELGF_ID = E.ELGF_ID),
    rx_recoveries
    AS (
    SELECT ma_num,
        cert_num,
        first_nm,
        last_nm,
        patient_dob_dt,
        ssn_num,
        group_cd,
        contract_num,
        carrier_cd,
        MAX(from_dos_dt) AS FROM_DOS_YR,
        SUM(remit_amt)                 AS REMIT_AMT,
        SUM(ma_paid_amt)               AS MA_PAID
        FROM edw_ar_fl.artbase
        WHERE project_cd >= '31'
            AND  from_dos_dt >= ADD_MONTHS(CURRENT_DATE, -36)
            AND  claim_type_cd = '12'
            AND  transact_status_cd = 'B'
            AND  remit_amt > 1000
        GROUP  BY 1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9
    ),
    med_recoveries
    AS (
    SELECT ma_num,
        cert_num,
        first_nm,
        last_nm,
        patient_dob_dt,
        ssn_num,
        group_cd,
        contract_num,
        carrier_cd,
        MAX(from_dos_dt) AS FROM_DOS_YR,
        SUM(remit_amt)                 AS REMIT_AMT,
        SUM(ma_paid_amt)               AS MA_PAID
        FROM edw_ar_fl.artbase
        WHERE project_cd >= '31'
            AND  from_dos_dt >= ADD_MONTHS(CURRENT_DATE, -36)
            AND  claim_type_cd NOT IN ( '11', '12' )
            AND  transact_status_cd IN ( 'A', 'B' ) --and remit_amt >0 
            
            AND  ma_num IN (
        SELECT ma_num
            FROM   rx_recoveries)
        GROUP  BY 1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9
    ),
    RX_NO_MED AS (
    SELECT R.ma_num,
        R.cert_num,
        R.first_nm,
        R.last_nm,
        R.patient_dob_dt,
        R.ssn_num,
        R.group_cd,
        R.contract_num,
        R.carrier_cd as rx_carrier_cd,
        t.carrier_cd as med_carrier_cd,
        R.from_dos_yr,
        R.remit_amt,
        R.ma_paid,
        NB2.dependent_first_nm,
        NB2.dependent_last_nm,
        NB2.EMPLOYER_NM,
        NB2.dependent_birth_dt,
        NB2.dependent_ssn_num,
        NB2.carrier_count,
        NB2.latest_med_elig_start_date,
        NB2.latest_med_elig_stop_date,
        NB2.latest_phar_elig_start_date,
        NB2.latest_phar_elig_stop_date
        FROM rx_recoveries r
        LEFT JOIN med_recoveries t
        ON r.ma_num = t.ma_num
        LEFT JOIN (
        SELECT NB.dependent_first_nm,
            NB.dependent_last_nm,
            NB.dependent_birth_dt,
            NB.EMPLOYER_NM,
            NB.dependent_ssn_num,
            COUNT(DISTINCT NB.carrier_cd) AS CARRIER_COUNT,
            MAX(NB.med_elig_start_dt)     AS
            LATEST_MED_ELIG_START_DATE,
            MAX(NB.med_elig_stop_dt)      AS
            LATEST_MED_ELIG_STOP_DATE,
            MAX(NB.phar_elig_start_dt)    AS
            LATEST_PHAR_ELIG_START_DATE,
            MAX(NB.phar_elig_stop_dt)     AS
            LATEST_PHAR_ELIG_STOP_DATE
            FROM edw_elg_fl.ned_525_base NB
            WHERE NB.create_dt >= ADD_MONTHS(CURRENT_DATE, -36)
                AND  ( NB.med_benefit_flag = 'Y'
            OR NB.phar_benefit_flag = 'Y' )
                AND  NB.segment_type_cd IN ( '019', '013' )
            GROUP  BY 1,
                2,
                3,
                4,
                5) NB2
        ON R.first_nm = NB2.dependent_first_nm
        AND R.last_nm = NB2.dependent_last_nm
        AND R.patient_dob_dt = NB2.dependent_birth_dt
        AND R.ssn_num = NB2.dependent_ssn_num
        WHERE t.ma_num IS NULL
            AND  r.carrier_cd LIKE '%MAXOR%'
            OR  r.carrier_cd LIKE 'PTNRX'
            OR  r.carrier_cd LIKE 'RXEDO'
            OR  r.carrier_cd LIKE 'RXDMI'
            OR  r.carrier_cd LIKE 'BCMSR'
            OR  r.carrier_cd LIKE 'CTRX'
            OR  r.carrier_cd LIKE 'NAVRX'
            OR  r.carrier_cd LIKE 'PRESO'
            OR  r.carrier_cd LIKE 'SXC'
            OR  r.carrier_cd LIKE 'APM'
            OR  r.carrier_cd LIKE 'ARGUS'
            OR  r.carrier_cd LIKE 'RXOPT'
            OR  r.carrier_cd LIKE 'LDIRX'
            OR  r.carrier_cd LIKE 'PRIME'
            OR  r.carrier_cd LIKE 'MEDIM'
            OR  r.carrier_cd LIKE 'BCARD'
            OR  r.carrier_cd LIKE 'USRPT'
            OR  r.carrier_cd LIKE 'MEDIM'
            OR  r.carrier_cd LIKE 'PAID'
        GROUP  BY 1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            20,
            21,
            22,
            23),
    DUPLICATE_SUBMISSION AS (
    SELECT	   J.ma_num,
        J.cert_num,
        J.first_nm,
        J.last_nm,
        J.EMPLOYER_NM,
        J.patient_dob_dt,
        J.ssn_num,
        J.group_cd,
        J.contract_num,
        J.rx_carrier_cd,
        J.med_carrier_cd,
        J.from_dos_yr,
        J.remit_amt,
        J.ma_paid,
        J.dependent_first_nm,
        J.dependent_last_nm,
        J.dependent_birth_dt,
        J.dependent_ssn_num,
        J.carrier_count,
        J.latest_med_elig_start_date,
        J.latest_med_elig_stop_date,
        J.latest_phar_elig_start_date,
        J.latest_phar_elig_stop_date
        FROM DEDUPE AS G
        LEFT JOIN RX_NO_MED AS J
        ON G.DG_RECORDS_SSN = J.ssn_num
        AND G.DG_RECORDS_LAST_NM = J.last_nm
        AND G.DG_RECORDS_FIRST_NM = J.first_nm
        AND G.DG_MA_NUM = J.MA_NUM
        WHERE G.DG_RECORDS_SSN IS NOT NULL
            AND  J.ssn_num IS NOT NULL),
    FINAL_RUN AS (
    SELECT 		   S.ma_num,
        S.cert_num,
        S.first_nm,
        S.last_nm,
        S.EMPLOYER_NM,
        S.patient_dob_dt,
        S.ssn_num,
        S.group_cd,
        S.contract_num,
        S.rx_carrier_cd,
        S.med_carrier_cd,
        S.from_dos_yr,
        S.remit_amt,
        S.ma_paid,
        S.dependent_first_nm,
        S.dependent_last_nm,
        S.dependent_birth_dt,
        S.dependent_ssn_num,
        S.carrier_count,
        S.latest_med_elig_start_date,
        S.latest_med_elig_stop_date,
        S.latest_phar_elig_start_date,
        S.latest_phar_elig_stop_date,
        
        CASE
            WHEN K.ssn_num = S.ssn_num AND K.ma_num = S.ma_num 	AND K.first_nm = S.first_nm	AND K.last_nm = S.last_nm THEN 'DUPLICATE' ELSE 'PASSED'
        END AS DUPE_CHECK
        FROM RX_NO_MED AS S
        LEFT JOIN DUPLICATE_SUBMISSION AS K
        ON K.ssn_num = S.ssn_num
        AND K.ma_num = S.ma_num
        AND K.first_nm = S.first_nm
        AND K.last_nm = S.last_nm
        WHERE DUPE_CHECK ='PASSED')
    SELECT 
        U.ma_num,
        U.cert_num,
        U.first_nm,
        U.last_nm,
        U.EMPLOYER_NM,
        U.patient_dob_dt,
        U.ssn_num,
        U.group_cd,
        U.contract_num,
        U.rx_carrier_cd,
        U.med_carrier_cd,
        U.from_dos_yr,
        U.remit_amt,
        U.ma_paid,
        U.dependent_first_nm,
        U.dependent_last_nm,
        U.dependent_birth_dt,
        U.dependent_ssn_num,
        U.carrier_count,
        U.latest_med_elig_start_date,
        U.latest_med_elig_stop_date,
        U.latest_phar_elig_start_date,
        U.latest_phar_elig_stop_date
        FROM FINAL_RUN AS U

	
	
	
