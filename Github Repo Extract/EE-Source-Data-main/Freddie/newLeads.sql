WITH DEDUPE AS
(
          SELECT
                    --Pull in additional information for each life from AS DG_RECORDS table
                    F.RECIP_FIRST_NM  AS DG_RECORDS_FIRST_NM,
                    F.RECIP_LAST_NM   AS DG_RECORDS_LAST_NM,
                    F.RECIP_SSN_NUM   AS DG_RECORDS_SSN,
                    F.POLICY_NUM      AS POLICY_NUM,
                    F.RECIP_MA_NUM    AS DG_MA_NUM,
                    F.POLICY_START_DT AS DG_POLICY_START,
                    F.POLICY_END_DT   AS DG_POLICY_END,
                    --Pull in additional information for each life from AS DG_MATCHES table
                    E.POLICY_NUM       AS DG_MATCHES_POLICY,
                    E.INSURED_SSN_NUM  AS DG_MATCHES_SSN,
                    E.INSURED_FIRST_NM AS DG_MATCHES_FIRST_NM,
                    E.INSURED_LAST_NM  AS DG_MATCHES_LAST_NM,
                    --Project Columns
                    P.CONTEXT_CD,
                    P.PROJECT_ID,
                    Cast (P.START_DTM AS DATE) AS RUN_DT,
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
                              WHEN V.CLIENT_CD IS NOT NULL
                              AND       V.VERST_RF IN ('NOLOAD',
                                                       'COVRXWALK') THEN 'REJECT'
                              ELSE 'PASSED'
                    END AS VERIF_ROUTING_CHECK,
                    CASE
                              WHEN V.CLIENT_CD IS NOT NULL
                              AND       V.VERST_RF IN ('ETR_QUEUED',
                                                       'WEB_NEWLD',
                                                       'UNASGND',
                                                       'FLATQUEUED') THEN 'PENDING'
                              ELSE 'PROCESSED'
                    END AS VERIF_PENDING_CHECK,
                    CASE
                              WHEN V.CLIENT_CD IS NOT NULL
                              AND       V.VERST_RF IN ('DUPLICATES') THEN 'DUPLICATE'
                              ELSE 'UNIQUE'
                    END AS VERIF_DUPE_CHECK,
                    CASE
                              WHEN V.CLIENT_CD IS NOT NULL
                              AND       V.VERST_RF IN ('ONHOLD',
                                                       'ECAREONHLD',
                                                       'CARACCISUE',
                                                       NULL) THEN 'ON HOLD'
                              ELSE 'ACTIVE'
                    END AS VERIF_HOLD_CHECK,
                    CASE
                              WHEN V.CLIENT_CD IS NOT NULL
                              AND       V.VERST_RF IN ('MCHINVLD',
                                                       'BAD_DATA',
                                                       'MISINFO',
                                                       'MIS_ELGFID') THEN 'DATA ERROR'
                              ELSE 'PASSED'
                    END AS VERIF_DATA_CHECK,
                    CASE
                              WHEN V.CLIENT_CD IS NOT NULL
                              AND       V.VERST_RF IN ('ETR_CMPLTD',
                                                       'EDIQCCOMPL',
                                                       'FLATFILE',
                                                       'MATCH_NED',
                                                       'SKP_ECARE') THEN 'COMPLETE'
                              ELSE 'INCOMPLETE'
                    END AS VERIF_COMP_CHECK,
                    --FUSP Delivery Detail Fields
                    CASE
                              WHEN F.VERST_RF IN ('ELGVER',
                                                  'SKP_ECARE',
                                                  'ELGTERM') THEN 'VALID POLICY'
                              WHEN F.VERST_RF IS NULL THEN 'NO VALUE'
                              ELSE 'INVALID POLICY'
                    END AS DELIV_VALID_CHECK,
                    CASE
                              WHEN F.CONTEXT_CD LIKE '%MFS%'
                              AND       F.NEW_CONTEXT_CD IS NOT NULL THEN 'MOVED TO YLD'
                              WHEN F.CONTEXT_CD LIKE '%SPL%' THEN 'NO MOVE NEEDED'
                              ELSE 'NOT MOVED'
                    END AS DELIV_FLIP_CHECK,
                    CASE
                              WHEN F.CONTEXT_CD LIKE '%SPL%'
                              AND       F.VRFRV_RF = '000' THEN 'PENDING BACKEND'
                              WHEN F.YLD_CONTEXT_CD IS NOT NULL
                              AND       F.YLD_VRFRV_RF = '000' THEN 'PENDING BACKEND'
                              WHEN F.VRFRV_RF = '110'
                              OR        F.YLD_VRFRV_RF = '110' THEN 'CLEARED BACKEND'
                              WHEN F.CONTEXT_CD LIKE '%MFS%'
                              AND       F.YLD_CONTEXT_CD IS NULL THEN 'PENDING FLIP'
                              ELSE 'REJECTED'
                    END AS DELIV_BE_CHECK,
                    CASE
                              WHEN (
                                                  F.VRFRV_RF = '110'
                                        AND       Upper(Ltrim(Rtrim(F.VRFRV_CMT))) LIKE '%DELIV%')
                              OR        (
                                                  F.YLD_VRFRV_RF = '110'
                                        AND       Upper(Ltrim(Rtrim(F.YLD_VRFRV_CMT))) LIKE '%DELIV%') THEN 'DELIVERED'
                              ELSE 'NOT DELIVERED'
                    END DELIV_TRANSMIT_CHECK,
                    --NEDB Detail
                    CASE
                              WHEN N.SEGMENT_ID IS NOT NULL THEN 'FUS2NED SUCCESS'
                              ELSE 'FUS2NED PENDING'
                    END AS NED_SEGMENT_CHECK,
                    CASE
                              WHEN N.SEGMENT_ID IS NOT NULL
                              AND       F.YLD_POSTING_ID IS NOT NULL THEN Ltrim(Rtrim(F.CLIENT_CD))
                                                  ||Ltrim(Rtrim(F.YLD_CONTEXT_CD))
                                                  ||Cast(F.YLD_POSTING_ID AS VARCHAR(16))
                              WHEN N.SEGMENT_ID IS NOT NULL
                              AND       F.CONTEXT_CD LIKE '%SPL%' THEN Ltrim(Rtrim(F.CLIENT_CD))
                                                  ||Ltrim(Rtrim(F.CONTEXT_CD))
                                                  ||Cast(F.POSTING_ID AS VARCHAR(16))
                              ELSE 'PENDING TRACE_CD'
                    END AS NED_TRACE_CD,
                    --ELGF Detail
                    CASE
                              WHEN N.SEGMENT_ID IS NOT NULL
                              AND       E.ELGF_ID IS NULL THEN 'PENDING MATCH'
                              WHEN N.SEGMENT_ID IS NOT NULL
                              AND       E.ELGF_ID IS NOT NULL THEN 'MATCHED'
                              ELSE 'N/A'
                    END AS ELG_MATCH_CHECK,
                    --Billing Detail
                    CASE
                              WHEN B.ELGF_ID IS NULL THEN 'NO BILLINGS'
                              WHEN B.ELGF_ID IS NOT NULL THEN 'CLAIMS BILLED'
                              ELSE 'N/A'
                    END AS BILL_HIT_CHECK,
                    CASE
                              WHEN B.ELGF_ID IS NOT NULL
                              AND       B.RECOVERED_AMT > 0 THEN 'RECOVERED'
                              ELSE 'NO RECOVERIES'
                    END                           AS BILL_RECOV_CHECK
          FROM      DL_DL_OPS_MET.PROJECTS        AS P
          LEFT JOIN DL_DL_OPS_MET.DG_VERIFICATION AS V
          ON        P.CONTEXT_CD = V.CONTEXT_CD
          AND       P.PROJECT_ID = V.PROJECT_ID
          LEFT JOIN DL_DL_OPS_MET.DG_RECORDS AS F
          ON        V.CONTEXT_CD = F.CONTEXT_CD
          AND       V.COVERAGE_ID = F.COVERAGE_ID
          AND       V.CLIENT_CD = F.CLIENT_CD
          LEFT JOIN DL_DL_OPS_MET.V9_SOURCE AS N
          ON        N.CARRIER_CD = F.CARRIER_CD
          AND       N.DEPENDENT_SSN_NUM = F.RECIP_SSN_NUM
          AND       F.POLICY_NUM = N.POLICY_NUM
          LEFT JOIN DL_DL_OPS_MET.DG_MATCHES AS E
          ON        N.SEGMENT_ID = E.SEGMENT_ID
          LEFT JOIN
                    (
                             SELECT   SEGMENT_ID,
                                      ELGF_ID,
                                      CLIENT_CD,
                                      CARRIER_CD,
                                      Sum(CLAIM_REMIT_AMT) AS RECOVERED_AMT
                             FROM     DL_DL_OPS_MET.DG_BILLINGS
                             GROUP BY 1,
                                      2,
                                      3,
                                      4 ) AS B
          ON        B.ELGF_ID = E.ELGF_ID), 
          --ADDED RX RECOVERY TABLE--
          RX_RECOVERIES AS
(
       SELECT *
       FROM   DL_EMP_TBL.RX_RECOVERIES), 
       --ADDED MED RECOVERY TABLE--
       MED_RECOVERIES AS
(
       SELECT *
       FROM   DL_EMP_TBL.MED_RECOVERIES ), 
       --ADDED NEDB RECOVERY TABLE--
       NEDB AS
(
       SELECT *
       FROM   DL_EMP_TBL.NEDB_RECOVERIES), 
       --ADDED ELGF RECOVERY TABLE--
       ELGF AS
(
          SELECT    '1' AS ROW_ID,
                    E.CLIENT_CD,
                    E.RI_MA_NUM,
                    E.DP_LAST_NM,
                    E.PH_LAST_NM,
                    E.DP_FIRST_NM,
                    E.PH_FIRST_NM,
                    E.DP_GENCD_RF,
                    E.PH_GENCD_RF,
                    E.PH_DOB_DT,
                    E.DP_DOB_DT,
                    E.PH_SSN_NUM,
                    E.DP_SSN_NUM,
                    E.CARRIER_CD,
                    E.POLICY_NUM,
                    'RX' AS COVERAGE_TYPE_CD,
                    E.PH_ADDRESS1_TXT,
                    E.PH_CITY_TXT,
                    E.PH_STATE_CD,
                    E.PH_ZIP_CD,
                    E.CARRIER_OFFICE_CD,
                    E.GROUP_NUM,
                    E.ELGF_ID,
                    E.RSHCD_RF,
                    E.SEGMENT_ID
          FROM      RX_RECOVERIES AS R
          LEFT JOIN EDW_ELG_FL.ARTELGF       AS E
          ON        E.CLIENT_CD = R.CLIENT_CD
          AND       R.MA_NUM = E.RI_MA_NUM
          WHERE     E.RI_MA_NUM IS NOT NULL), 
          --RX RECORDS WITH NO MED--
          RX_NO_MED AS
(
                SELECT DISTINCT '1' AS LEAD_ID,
                                R.CLIENT_CD,
                                R.MA_NUM,
                                E.DP_LAST_NM,
                                E.PH_LAST_NM,
                                E.DP_FIRST_NM,
                                E.PH_FIRST_NM,
                                E.DP_GENCD_RF,
                                E.PH_GENCD_RF,
                                E.PH_DOB_DT,
                                E.DP_DOB_DT,
                                E.PH_SSN_NUM,
                                --RX Carrier Code Inserted Here--
                                R.CARRIER_CD AS RX_CARRIER_CD
                                --Med Carrier Code Inserted Here
                                EMP.MED_CARRIER_CD AS MED_CARRIER_CD,
                                --------------------------------
                                E.POLICY_NUM,
                                'MED'           AS COVERAGE_TYPE_CD,
                                N.ELIG_START_DT AS COVERAGE_START_DT,
                                N.ELIG_STOP_DT  AS COVERAGE_END_DT,
                                E.PH_ADDRESS1_TXT,
                                E.PH_CITY_TXT,
                                E.PH_STATE_CD,
                                E.PH_ZIP_CD,
                                E.CARRIER_OFFICE_CD,
                                E.GROUP_NUM,
                                E.ELGF_ID,
                                E.RSHCD_RF,
                                E.DP_SSN_NUM                         AS HDR_SSN_NUM,
                                NULL                                 AS REJ_QA,
                                'Direct Bill Gaps Maxor,NavRx, etc.' AS CMT_TXT,
                                'RX'                                 AS LEAD_COVTP,
                                N.EMPLOYER_NM                        AS LEAD_EMPLOYER,
                                EMP.RX_CARRIER_CD                    AS LEAD_CARRIER,
                                EMP.RX_GROUP_NUM                     AS LEAD_GROUP_NUM,
                                E.POLICY_NUM                         AS LEAD_POLICY_NUM
                FROM            RX_RECOVERIES R
                LEFT JOIN       MED_RECOVERIES M
                ON              R.CLIENT_CD = M.CLIENT_CD
                AND             R.MA_NUM = M.MA_NUM
                LEFT JOIN       ELGF E
                ON              R.CLIENT_CD = E.CLIENT_CD
                AND             R.MA_NUM = E.RI_MA_NUM
                LEFT JOIN       NEDB N
                ON              E.SEGMENT_ID = N.SEGMENT_ID
                LEFT JOIN
                                (
                                         SELECT   RX_CARRIER_CD,
                                                  RX_GROUP_NUM,
                                                  MED_CARRIER_CD,
                                                  TOTAL_CNT
                                         FROM     DL_EMP_TBL.EMP_SOT QUALIFY ROW_NUMBER() OVER (PARTITION BY RX_CARRIER_CD, RX_GROUP_NUM ORDER BY TOTAL_CNT DESC) = 1 ) EMP
                ON              R.GROUP_CD = EMP.RX_GROUP_NUM
                AND             R.CARRIER_CD = EMP.RX_CARRIER_CD
                WHERE           M.MA_NUM IS NULL
                AND             EMP.MED_CARRIER_CD IS NOT NULL
                AND             R.CARRIER_CD IN ('MAXOR',
                                                 'PTNRX',
                                                 'RXEDO',
                                                 'RXDMI',
                                                 'BCMSR',
                                                 'CTRX',
                                                 'NAVRX',
                                                 'PRESO',
                                                 'SXC',
                                                 'APM',
                                                 'ARGUS',
                                                 'RXOPT',
                                                 'LDIRX',
                                                 'MEDIM',
                                                 'BCARD',
                                                 'USRPT')), 
                                                 --GET MATCHES ON REPORTING QRY--
                                                 --HERE WE CAN FILTER TO KEEP THE MATCHES THAT HAD ROUTING ERRORS OR MAKE SECOND GUESSES ON THE MED CARRIER CODE--
                                                 DUPLICATE_SUBMISSION AS
(
          SELECT    J.MA_NUM,
                    J.CERT_NUM,
                    J.FIRST_NM,
                    J.LAST_NM,
                    J.EMPLOYER_NM,
                    J.PATIENT_DOB_DT,
                    J.SSN_NUM,
                    J.GROUP_CD,
                    J.CONTRACT_NUM,
                    J.RX_CARRIER_CD,
                    J.MED_CARRIER_CD,
                    J.FROM_DOS_YR,
                    J.REMIT_AMT,
                    J.MA_PAID,
                    J.DEPENDENT_FIRST_NM,
                    J.DEPENDENT_LAST_NM,
                    J.DEPENDENT_BIRTH_DT,
                    J.DEPENDENT_SSN_NUM,
                    J.CARRIER_COUNT,
                    J.LATEST_MED_ELIG_START_DATE,
                    J.LATEST_MED_ELIG_STOP_DATE,
                    J.LATEST_PHAR_ELIG_START_DATE,
                    J.LATEST_PHAR_ELIG_STOP_DATE,
                    G.VERIFICATION_STATUS,
					G.VERIFICATION_RESULT,
					G.VERIF_ROUTING_CHECK,
					G.VERIF_PENDING_CHECK,
					G.VERIF_DUPE_CHECK,
					G.VERIF_HOLD_CHECK,
					G.VERIF_DATA_CHECK,
					G.VERIF_COMP_CHECK,
					G.DELIV_VALID_CHECK
          FROM      DEDUPE    AS G
          LEFT JOIN RX_NO_MED AS J
          ON        G.DG_RECORDS_SSN = J.PH_SSN_NUM
          AND       G.DG_RECORDS_LAST_NM = J.PH_LAST_NM
          AND       G.DG_RECORDS_FIRST_NM = J.PH_FIRST_NM
          AND       G.DG_MA_NUM = J.MA_NUM
          WHERE     G.DG_RECORDS_SSN IS NOT NULL
          AND       J.PH_SSN_NUM IS NOT NULL), 
          
          FINAL_RUN AS
( SELECT
       SELECT DISTINCT S.LEAD_ID,
                       S.CLIENT_CD,
                       S.MA_NUM,
                       S.DP_LAST_NM,
                       S.PH_LAST_NM,
                       S.DP_FIRST_NM,
                       S.PH_FIRST_NM,
                       S.DP_GENCD_RF,
                       S.PH_GENCD_RF,
                       S.PH_DOB_DT,
                       S.DP_DOB_DT,
                       S.PH_SSN_NUM,
                       S.CARRIER_CD,
                       S.POLICY_NUM,
                       S.COVERAGE_TYPE_CD,
                       S.COVERAGE_START_DT,
                       S.COVERAGE_END_DT,
                       S.PH_ADDRESS1_TXT,
                       S.PH_CITY_TXT,
                       S.PH_STATE_CD,
                       S.PH_ZIP_CD,
                       S.CARRIER_OFFICE_CD,
                       S.GROUP_NUM,
                       S.ELGF_ID,
                       S.RSHCD_RF,
                       S.HDR_SSN_NUM,
                       S.REJ_QA,
                       S.CMT_TXT,
                       S.LEAD_COVTP,
                       S.LEAD_EMPLOYER,
                       S.LEAD_CARRIER,
                       S.LEAD_GROUP_NUM,
                       S.LEAD_POLICY_NUM,
                       CASE
                                       WHEN K.SSN_NUM = S.SSN_NUM
                                       AND             K.MA_NUM = S.MA_NUM
                                       AND             K.FIRST_NM = S.FIRST_NM
                                       AND             K.LAST_NM = S.LAST_NM THEN 'DUPLICATE'
                                       ELSE 'PASSED'
                       END                  AS DUPE_CHECK
       FROM            RX_NO_MED            AS S
       LEFT JOIN       DUPLICATE_SUBMISSION AS K
       ON              K.SSN_NUM = S.PH_SSN_NUM
       AND             K.MA_NUM = S.MA_NUM
       AND             K.FIRST_NM = S.PH_FIRST_NM
       AND             K.LAST_NM = S.PH_LAST_NM
       WHERE           DUPE_CHECK ='PASSED')
SELECT U.LEAD_ID,
       U.CLIENT_CD,
       U.MA_NUM,
       U.DP_LAST_NM,
       U.PH_LAST_NM,
       U.DP_FIRST_NM,
       U.PH_FIRST_NM,
       U.DP_GENCD_RF,
       U.PH_GENCD_RF,
       U.PH_DOB_DT,
       U.DP_DOB_DT,
       U.PH_SSN_NUM,
       U.CARRIER_CD,
       U.POLICY_NUM,
       U.COVERAGE_TYPE_CD,
       U.COVERAGE_START_DT,
       U.COVERAGE_END_DT,
       U.PH_ADDRESS1_TXT,
       U.PH_CITY_TXT,
       U.PH_STATE_CD,
       U.PH_ZIP_CD,
       U.CARRIER_OFFICE_CD,
       U.GROUP_NUM,
       U.ELGF_ID,
       U.RSHCD_RF,
       U.HDR_SSN_NUM,
       U.REJ_QA,
       U.CMT_TXT,
       U.LEAD_COVTP,
       U.LEAD_EMPLOYER,
       U.LEAD_CARRIER,
       U.LEAD_GROUP_NUM,
       U.LEAD_POLICY_NUM,
       U.DUPE_CHECK
FROM   FINAL_RUN AS U