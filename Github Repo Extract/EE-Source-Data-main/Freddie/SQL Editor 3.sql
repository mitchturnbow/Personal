SELECT *
FROM   dl_dl_ops_met.final_dg b
WHERE  (
              b.context_cd,b.project_id) in
       (
              SELECT context_cd,
                     project_id
              FROM   dl_dl_ops_met.projects
              WHERE  comment_txt IN ('031_FUSSPLBS_EMPLOY12072022.txt',
                                     '061_FUSSPLJS_EMPLOY12072022.txt',
                                     '093_FUSSPLLS_EMPLOY12072022.txt',
                                     '10X_FUSSPL1T_EMPLOY12072022.txt',
                                     '127_FUSSPLB4_EMPLOY12072022.txt',
                                     '128_FUSSPLB5_EMPLOY12072022.txt',
                                     '135_FUSSPLC2_EMPLOY12072022.txt',
                                     '172_FUSSPLG4_EMPLOY12072022.txt',
                                     '175_FUSSPLG7_EMPLOY12072022.txt',
                                     '180_FUSSPLH3_EMPLOY12072022.txt',
                                     '184_FUSSPLH7_EMPLOY12072022.txt',
                                     '187_FUSSPLJ0_EMPLOY12072022.txt',
                                     '196_FUSSPLJ9_EMPLOY12072022.txt',
                                     '212_FUSSPLL7_EMPLOY12072022.txt',
                                     '215_FUSSPLK4_EMPLOY12072022.txt',
                                     '223_FUSSPLL9_EMPLOY12072022.txt',
                                     '234_FUSSPLN2_EMPLOY12072022.txt',
                                     '235_FUSSPLN3_EMPLOY12072022.txt',
                                     '236_FUSSPLN4_EMPLOY12072022.txt',
                                     '239_FUSSPLN7_EMPLOY12072022.txt',
                                     '251_FUSSPLR3_EMPLOY12072022.txt',
                                     '255_FUSSPLR7_EMPLOY12072022.txt',
                                     '272_FUSSPLT4_EMPLOY12072022.txt',
                                     '276_FUSSPLT7_EMPLOY12072022.txt',
                                     '279_FUSSPLT9_EMPLOY12072022.txt',
                                     '280_FUSSPLP1_EMPLOY12072022.txt',
                                     '290_FUSSPLQ2_EMPLOY12072022.txt',
                                     '296_FUSSPLU2_EMPLOY12072022.txt',
                                     '301_FUSSPLLA_EMPLOY12072022.txt',
                                     '303_FUSSPLNJ_EMPLOY12072022.txt',
                                     '305_FUSSPLMO_EMPLOY12072022.txt',
                                     '308_FUSSPLVA_EMPLOY12072022.txt',
                                     '309_FUSSPLMI_EMPLOY12072022.txt',
                                     '310_FUSSPLMD_EMPLOY12072022.txt',
                                     '312_FUSSPLPA_EMPLOY12072022.txt',
                                     '313_FUSSPLFL_EMPLOY12072022.txt',
                                     '315_FUSSPLMA_EMPLOY12072022.txt',
                                     '316_FUSSPLIA_EMPLOY12072022.txt',
                                     '317_FUSSPLCA_EMPLOY12072022.txt',
                                     '319_FUSSPLCT_EMPLOY12072022.txt',
                                     '320_FUSSPLNY_EMPLOY12072022.txt',
                                     '321_FUSSPLIN_EMPLOY12072022.txt',
                                     '324_FUSSPLME_EMPLOY12072022.txt',
                                     '326_FUSSPLCO_EMPLOY12072022.txt',
                                     '330_FUSSPLAR_EMPLOY12072022.txt',
                                     '335_FUSSPLKS_EMPLOY12072022.txt',
                                     '341_FUSSPLOH_EMPLOY12072022.txt',
                                     '357_FUSSPLOK_EMPLOY12072022.txt',
                                     '358_FUSSPLNV_EMPLOY12072022.txt',
                                     '359_FUSSPLNM_EMPLOY12072022.txt',
                                     '367_FUSSPLMW_EMPLOY12072022.txt',
                                     '374_FUSSPLPP_EMPLOY12072022.txt',
                                     '386_FUSSPLPY_EMPLOY12072022.txt',
                                     '389_FUSSPLUP_EMPLOY12072022.txt',
                                     '395_FUSSPLNC_EMPLOY12072022.txt',
                                     '396_FUSSPLGA_EMPLOY12072022.txt',
                                     '408_FUSSPLIZ_EMPLOY12072022.txt',
                                     '427_FUSSPLUT_EMPLOY12072022.txt',
                                     '432_FUSSPLVK_EMPLOY12072022.txt',
                                     '444_FUSSPLMS_EMPLOY12072022.txt',
                                     '448_FUSSPLFS_EMPLOY12072022.txt',
                                     '455_FUSSPLFV_EMPLOY12072022.txt',
                                     '478_FUSSPLOR_EMPLOY12072022.txt',
                                     '481_FUSSPLDQ_EMPLOY12072022.txt',
                                     '482_FUSSPLGM_EMPLOY12072022.txt',
                                     '483_FUSSPLGN_EMPLOY12072022.txt',
                                     '496_FUSSPLHG_EMPLOY12072022.txt',
                                     '497_FUSSPLHQ_EMPLOY12072022.txt',
                                     '498_FUSSPLNF_EMPLOY12072022.txt',
                                     '501_FUSSPLWQ_EMPLOY12072022.txt',
                                     '516_FUSSPLNI_EMPLOY12072022.txt',
                                     '524_FUSSPLTK_EMPLOY12072022.txt',
                                     '525_FUSSPLPK_EMPLOY12072022.txt',
                                     '527_FUSSPLMN_EMPLOY12072022.txt',
                                     '535_FUSSPLCX_EMPLOY12072022.txt',
                                     '551_FUSSPLAQ_EMPLOY12072022.txt',
                                     '552_FUSSPLAW_EMPLOY12072022.txt',
                                     '555_FUSSPLAT_EMPLOY12072022.txt',
                                     '556_FUSSPLAF_EMPLOY12072022.txt',
                                     '557_FUSSPLAY_EMPLOY12072022.txt',
                                     '561_FUSSPLWF_EMPLOY12072022.txt',
                                     '564_FUSSPLTN_EMPLOY12072022.txt',
                                     '569_FUSSPLWI_EMPLOY12072022.txt',
                                     '573_FUSSPLBM_EMPLOY12072022.txt',
                                     '577_FUSSPLWG_EMPLOY12072022.txt',
                                     '585_FUSSPLAZ_EMPLOY12072022.txt',
                                     '586_FUSSPLAK_EMPLOY12072022.txt',
                                     '590_FUSSPLAL_EMPLOY12072022.txt',
                                     '600_FUSSPLID_EMPLOY12072022.txt',
                                     '619_FUSSPLPM_EMPLOY12072022.txt',
                                     '623_FUSSPLGH_EMPLOY12072022.txt',
                                     '632_FUSSPLDR_EMPLOY12072022.txt',
                                     '645_FUSSPLWV_EMPLOY12072022.txt',
                                     '647_FUSSPLCB_EMPLOY12072022.txt',
                                     '652_FUSSPLOP_EMPLOY12072022.txt',
                                     '659_FUSSPLNP_EMPLOY12072022.txt',
                                     '663_FUSSPLSM_EMPLOY12072022.txt',
                                     '700_FUSSPLQE_EMPLOY12072022.txt',
                                     '708_FUSSPLQN_EMPLOY12072022.txt',
                                     '716_FUSSPLQV_EMPLOY12072022.txt',
                                     '718_FUSSPLDV_EMPLOY12072022.txt',
                                     '725_FUSSPLRD_EMPLOY12072022.txt',
                                     '738_FUSSPLEV_EMPLOY12072022.txt',
                                     '756_FUSSPLSK_EMPLOY12072022.txt',
                                     '777_FUSSPLTA_EMPLOY12072022.txt',
                                     '781_FUSSPLBE_EMPLOY12072022.txt',
                                     '804_FUSSPLWS_EMPLOY12072022.txt',
                                     '826_FUSSPLXS_EMPLOY12072022.txt',
                                     '828_FUSSPLXU_EMPLOY12072022.txt',
                                     '831_FUSSPLXR_EMPLOY12072022.txt',
                                     '833_FUSSPLXW_EMPLOY12072022.txt',
                                     '834_FUSSPLU7_EMPLOY12072022.txt',
                                     '842_FUSSPLEK_EMPLOY12072022.txt',
                                     '843_FUSSPLYF_EMPLOY12072022.txt',
                                     '855_FUSSPLF0_EMPLOY12072022.txt',
                                     '866_FUSSPLU8_EMPLOY12072022.txt',
                                     '867_FUSSPLU9_EMPLOY12072022.txt',
                                     '889_FUSSPLX4_EMPLOY12072022.txt',
                                     '905_FUSSPLYH_EMPLOY12072022.txt',
                                     '934_FUSSPLZD_EMPLOY12072022.txt',
                                     '954_FUSSPLEL_EMPLOY12072022.txt',
                                     '956_FUSSPL1B_EMPLOY12072022.txt',
                                     '988_FUSSPLGQ_EMPLOY12072022.txt',
                                     '995_FUSSPL22_EMPLOY12072022.txt') )