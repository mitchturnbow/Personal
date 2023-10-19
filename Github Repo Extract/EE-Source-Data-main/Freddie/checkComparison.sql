WITH leads AS
(
                SELECT DISTINCT '1' AS lead_id,
                                r.client_cd,
                                r.ma_num,
                                e.dp_last_nm,
                                e.ph_last_nm,
                                e.dp_first_nm,
                                e.ph_first_nm,
                                e.dp_gencd_rf,
                                e.ph_gencd_rf,
                                e.ph_dob_dt,
                                e.dp_dob_dt,
                                e.ph_ssn_num,
                                r.carrier_cd as rx_carrier_cd,
                                --Med Carrier Code Inserted Here
                                emp.med_carrier_cd AS med_carrier_cd,
                                --------------------------------
                                e.policy_num,
                                'MED'           AS coverage_type_cd,
                                n.elig_start_dt AS coverage_start_dt,
                                n.elig_stop_dt  AS coverage_end_dt,
                                e.ph_address1_txt,
                                e.ph_city_txt,
                                e.ph_state_cd,
                                e.ph_zip_cd,
                                e.carrier_office_cd,
                                e.group_num,
                                e.elgf_id,
                                e.rshcd_rf,
                                e.dp_ssn_num                         AS hdr_ssn_num,
                                NULL                                 AS rej_qa,
                                'Direct Bill Gaps Maxor,NavRx, etc.' AS cmt_txt,
                                'RX'                                 AS lead_covtp,
                                n.employer_nm                        AS lead_employer,
                                emp.rx_carrier_cd                    AS lead_carrier,
                                emp.rx_group_num                     AS lead_group_num,
                                e.policy_num                         AS lead_policy_num
                FROM            dl_emp_tbl.rx_recoveries R
                LEFT JOIN       dl_emp_tbl.med_recoveries M
                ON              r.client_cd = m.client_cd
                AND             r.ma_num = m.ma_num
                LEFT JOIN       dl_emp_tbl.elgf_recoveries E
                ON              r.client_cd = e.client_cd
                AND             r.ma_num = e.ri_ma_num
                LEFT JOIN       dl_emp_tbl.nedb_recoveries N
                ON              e.segment_id = n.segment_id
                LEFT JOIN
                                (
                                         SELECT   rx_carrier_cd,
                                                  rx_group_num,
                                                  med_carrier_cd,
                                                  total_cnt
                                         FROM     dl_emp_tbl.emp_sot QUALIFY row_number() over (partition by rx_carrier_cd, rx_group_num ORDER BY total_cnt DESC) = 1 ) emp
                ON              r.group_cd = emp.rx_group_num
                AND             r.carrier_cd = emp.rx_carrier_cd
                WHERE           m.ma_num IS NULL
                AND             emp.med_carrier_cd IS NOT NULL
                AND             r.carrier_cd IN ('MAXOR',
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
 leadsSecondAttempt AS
(
                SELECT DISTINCT '1' AS lead_id,
                                r.client_cd,
                                r.ma_num,
                                e.dp_last_nm,
                                e.ph_last_nm,
                                e.dp_first_nm,
                                e.ph_first_nm,
                                e.dp_gencd_rf,
                                e.ph_gencd_rf,
                                e.ph_dob_dt,
                                e.dp_dob_dt,
                                e.ph_ssn_num,
                                --Med Carrier Code Inserted Here
                                emp.med_carrier_cd AS carrier_cd,
                                --------------------------------
                                e.policy_num,
                                'MED'           AS coverage_type_cd,
                                n.elig_start_dt AS coverage_start_dt,
                                n.elig_stop_dt  AS coverage_end_dt,
                                e.ph_address1_txt,
                                e.ph_city_txt,
                                e.ph_state_cd,
                                e.ph_zip_cd,
                                e.carrier_office_cd,
                                e.group_num,
                                e.elgf_id,
                                e.rshcd_rf,
                                e.dp_ssn_num                         AS hdr_ssn_num,
                                NULL                                 AS rej_qa,
                                'Direct Bill Gaps Maxor,NavRx, etc.' AS cmt_txt,
                                'RX'                                 AS lead_covtp,
                                n.employer_nm                        AS lead_employer,
                                emp.rx_carrier_cd                    AS lead_carrier,
                                emp.rx_group_num                     AS lead_group_num,
                                e.policy_num                         AS lead_policy_num
                FROM            dl_emp_tbl.rx_recoveries R
                LEFT JOIN       dl_emp_tbl.med_recoveries M
                ON              r.client_cd = m.client_cd
                AND             r.ma_num = m.ma_num
                LEFT JOIN       dl_emp_tbl.elgf_recoveries E
                ON              r.client_cd = e.client_cd
                AND             r.ma_num = e.ri_ma_num
                LEFT JOIN       dl_emp_tbl.nedb_recoveries N
                ON              e.segment_id = n.segment_id
                LEFT JOIN
                                (
                                         SELECT   rx_carrier_cd,
                                                  rx_group_num,
                                                  med_carrier_cd,
                                                  total_cnt
                                         FROM     dl_emp_tbl.emp_sot QUALIFY row_number() over (partition by rx_carrier_cd, rx_group_num ORDER BY total_cnt DESC) = 1 ) emp
                ON              r.group_cd = emp.rx_group_num
                AND             r.carrier_cd = emp.rx_carrier_cd
                WHERE           m.ma_num IS NULL
                AND             emp.med_carrier_cd IS NOT NULL
                AND             r.carrier_cd IN ('MAXOR',
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
fupr AS
(
       SELECT *
       FROM   edw_elg_fl.artfupr AS b
       WHERE  (
                     context_cd LIKE 'FUSYLD%'
              OR     context_cd LIKE 'FUSMFS%'
              OR     context_cd LIKE 'FUSSPL%')
       AND    description_txt LIKE '%DATA_GAP%'
       AND    (
                     comment_txt LIKE '%EMPLOY%'
              OR     comment_txt LIKE '%SPLAT%')
       AND    prjst_rf = 'OPEN'
       AND    cast(start_dtm AS date) >= '2022-10-15'), 
covv_fuvr AS
(
          SELECT    '1' AS row_id,
                    p.context_cd,
                    p.project_id,
                    v.client_cd,
                    v.coverage_id,
                    v.load_dt,
                    v.carrier_cd,
                    v.carrier_office_cd,
                    v.dp_last_nm,
                    v.dp_first_nm,
                    v.dp_ssn_num,
                    v.ma_num,
                    v.policy_num,
                    v.ssn_num,
                    CASE
                              WHEN v.verst_rf = 'QUEUED' THEN mv.last_verst_rf
                              ELSE v.verst_rf
                    END  AS verst_rf
          FROM      fupr AS p
          LEFT JOIN
                    (
                           SELECT *
                           FROM   edw_ar_fl.artcovv
                           WHERE  load_dt >= CURRENT_DATE - 60
                           AND    (
                                         context_cd, project_id) IN
                                  (
                                         SELECT context_cd,
                                                project_id
                                         FROM   fupr)) AS v
          ON        p.context_cd = v.context_cd
          AND       p.project_id = v.project_id
          LEFT JOIN
                    (
                           SELECT *
                           FROM   edw_ar_fl.artfuvr
                           WHERE  dw_insert_timestamp >= CURRENT_DATE - 60) AS mv
          ON        mv.coverage_id = v.coverage_id
          AND       v.context_cd = mv.context_cd), 
fusp AS
(
          SELECT    f.client_cd,
                    f.context_cd,
                    f.project_id,
                    f.recip_ma_num,
                    f.recip_first_nm,
                    f.recip_last_nm,
                    f.recip_dob_dt,
                    f.recip_ssn_num,
                    f.carrier_cd,
                    f.policy_num,
                    f.plctp_rf,
                    f.policy_start_dt,
                    f.policy_end_dt,
                    f.verst_rf,
                    f.vrfrv_rf,
                    f.vrfrv_cmt,
                    f.verify_dt,
                    f.vrfrv_dt,
                    f.report_dt,
                    f.new_context_cd,
                    f.emp_nm,
                    f.posting_id,
                    f.coverage_id
          FROM      fupr               AS p
          LEFT JOIN edw_elg_fl.artfusp AS f
          ON        p.context_cd = f.context_cd
          AND       p.project_id = f.project_id
          WHERE     p.context_cd IS NOT NULL qualify row_number() OVER (partition BY f.client_cd, f.context_cd, f.posting_id ORDER BY f.client_cd, f.context_cd, f.posting_id) = 1),
prefix_cd as (
SELECT   med_carrier_cd,
         rx_carrier_cd,
         prefix_cd,
         cnt_num
FROM     dl_emp_tbl.bc_prefix QUALIFY row_number() OVER (partition BY rx_carrier_cd, med_carrier_cd ORDER BY cnt_num DESC) = 1)
/*
 * This qry gets all the new leads
 */     
          /*
SELECT    *
FROM      leads      AS f
LEFT JOIN covv_fuvr AS c
ON        c.dp_first_nm = f.dp_first_nm
AND       c.dp_last_nm = f.dp_last_nm
AND 	  c.dp_ssn_num = f.hdr_ssn_num
AND 	  c.ma_num = f.ma_num
AND 	  c.client_cd = f.client_cd
AND 	  c.carrier_cd = f.carrier_cd
WHERE	  c.dp_ssn_num IS NULL and f.hdr_ssn_num is not null and c.row_id is null
 */




--SELECT count(distinct(f.policy_num)) as leadCounts,count(distinct(C.policy_num)) as covvCounts
SELECT    f.lead_id,
          f.client_cd,
          f.ma_num,
          f.dp_last_nm,
          f.ph_last_nm,
          f.dp_first_nm,
          f.ph_first_nm,
          f.dp_gencd_rf,
          f.ph_gencd_rf,
          f.ph_dob_dt,
          f.dp_dob_dt,
          f.ph_ssn_num,
          f.rx_carrier_cd,
          f.med_carrier_cd,
          f.policy_num,
          f.coverage_type_cd,
          f.coverage_start_dt,
          f.coverage_end_dt,
          f.ph_address1_txt,
          f.ph_city_txt,
          f.ph_state_cd,
          f.ph_zip_cd,
          f.carrier_office_cd,
          f.group_num,
          f.elgf_id,
          f.rshcd_rf,
          f.hdr_ssn_num,
          f.rej_qa,
          f.cmt_txt,
          f.lead_covtp,
          f.lead_employer,
          f.lead_carrier,
          f.lead_group_num,
          f.lead_policy_num,
          c.row_id      AS row_id_covv,
          c.context_cd  AS context_cd_covv,
          c.project_id  AS project_id_covv,
          c.client_cd   AS client_cd_covv,
          c.coverage_id AS coverage_id_covv,
          c.load_dt     AS load_dt_covv,
          c.carrier_cd  AS carrier_cd_covv,
          c.carrier_office_cd,
          c.dp_last_nm,
          c.dp_first_nm,
          c.dp_ssn_num,
          c.ma_num,
          c.ssn_num,
          c.verst_rf,
          p.prefix_cd,
          CASE
                    WHEN c.client_cd IS NOT NULL
                    AND       c.verst_rf IN ( 'NOLOAD',
                                             'COVRXWALK' ) THEN 'REJECT'
                    ELSE 'PASSED'
          END AS verif_routing_check,
          CASE
                    WHEN c.client_cd IS NOT NULL
                    AND       c.verst_rf IN ( 'ETR_QUEUED',
                                             'WEB_NEWLD',
                                             'UNASGND',
                                             'FLATQUEUED' ) THEN 'PENDING'
                    ELSE 'PROCESSED'
          END AS verif_pending_check,
          CASE
                    WHEN c.client_cd IS NOT NULL
                    AND       c.verst_rf IN ( 'DUPLICATES' ) THEN 'DUPLICATE'
                    ELSE 'UNIQUE'
          END AS verif_dupe_check,
          CASE
                    WHEN c.client_cd IS NOT NULL
                    AND       c.verst_rf IN ( 'ONHOLD',
                                             'ECAREONHLD',
                                             'CARACCISUE',
                                             NULL ) THEN 'ON HOLD'
                    ELSE 'ACTIVE'
          END AS verif_hold_check,
          CASE
                    WHEN c.client_cd IS NOT NULL
                    AND       c.verst_rf IN ( 'MCHINVLD',
                                             'BAD_DATA',
                                             'MISINFO',
                                             'MIS_ELGFID' ) THEN 'DATA ERROR'
                    ELSE 'PASSED'
          END AS verif_data_check,
          CASE
                    WHEN c.client_cd IS NOT NULL
                    AND       c.verst_rf IN ( 'ETR_CMPLTD',
                                             'EDIQCCOMPL',
                                             'FLATFILE',
                                             'MATCH_NED',
                                             'SKP_ECARE' ) THEN 'COMPLETE'
                    ELSE 'INCOMPLETE'
          END AS verif_comp_check,
          CASE
                    WHEN c.policy_num IS NULL
                    AND       f.policy_num IS NOT NULL THEN 'NEW RECORD'
                    ELSE 'OLD'
          END       AS new_leads,
          p.prefix_cd
FROM      leads     AS f
LEFT JOIN covv_fuvr AS c
ON        c.client_cd = f.client_cd
AND       c.carrier_cd = f.med_carrier_cd
AND       c.ma_num = f.ma_num
AND       c.policy_num = f.policy_num
LEFT JOIN prefix_cd AS p 
on f.rx_carrier_cd = p.rx_carrier_cd 
and f.med_carrier_cd = p.med_carrier_cd






/*          
select *
FROM fupr AS P LEFT JOIN covv_fuvr AS V
ON P.CONTEXT_CD = V.CONTEXT_CD AND P.PROJECT_ID = V.PROJECT_ID
LEFT JOIN fusp AS F
ON P.CONTEXT_CD = F.CONTEXT_CD AND P.PROJECT_ID = F.PROJECT_ID 
WHERE V.CONTEXT_CD is null AND V.PROJECT_ID is null*/


/*
select *
FROM fupr AS P LEFT JOIN covv_fuvr AS V
ON P.CONTEXT_CD = V.CONTEXT_CD AND P.PROJECT_ID = V.PROJECT_ID
LEFT JOIN fusp AS F
ON V.CONTEXT_CD = F.CONTEXT_CD AND V.COVERAGE_ID = F.COVERAGE_ID AND V.CLIENT_CD = F.CLIENT_CD
WHERE f.coverage_id is null and f.context_cd is null and f.client_cd is null
and v.context_cd is null and v.project_id is null*/


