#########################################################################################################################################
#                                                                                                                                       #
#   Created by: Nandha Kumaran M marimn1                                                                                                #
#   Last Updated by   : Nandha Kumaran M marimn1                                                                                        #
#   Last Updated date : 08/13/2018                                                                                                      #
#   Script Name   : create_and_populate_ir_combined_limited_set_default_filters.py                                                      #
#   Description   : Full load of ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs                                                                 #
#   Log file path :/projects/equip/logs                                                                                                 #
#########################################################################################################################################

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import sys, time

#initalize SparkSession

hq = SparkSession.builder.appName("ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs").enableHiveSupport().getOrCreate()

hq.sql("DROP TABLE IF EXISTS equip.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs")

hq.sql("CREATE TABLE equip.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs \
STORED AS ORC  \
AS \
SELECT  \
vhcl.vin_id VIN_ID,  \
VHCL.EMSN_TYP_NM, \
VHCL.ENGN_PRFX_8_DGT_CD, \
VHCL.PRDCTN_MDL_SRS_CD, \
VHCL.NHTSA_VHCL_TYP_CD, \
CASE WHEN TRIM(vhcl.vhcl_mk_cd) = 'N' THEN 'Nissan' WHEN  TRIM(vhcl.vhcl_mk_cd) = 'I' THEN 'Infiniti' ELSE '' END vhcl_mk_nm,   \
VHCL.GLBL_MRKT_CD, \
VHCL.VHCL_FLT_IN, \
VHCL.CV_IN, \
VHCL.VHCL_CRSHD_IN, \
VHCL.RPT_EXCLSN_IN, \
VHCL.VHCL_LN_NM,  \
vhcl_yr_nb, \
VHCL.NML_PRDCTN_MDL_CD, \
upper(plnt_cd_nm) as plnt_cd_nm, \
CAST(from_unixtime(unix_timestamp(VHCL.ORGNL_IN_SVC_DT, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM-dd') AS DATE) ORGNL_IN_SVC_DT, \
CAST(from_unixtime(unix_timestamp(VHCL.IN_SVC_DT, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM-dd') AS DATE) IN_SVC_DT, \
from_unixtime(unix_timestamp(VHCL.MNFCTG_DT, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM') MNFCTG_DT_MNTH_YR, \
CAST(from_unixtime(unix_timestamp(VHCL.MNFCTG_DT, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM-dd') AS DATE) MNFCTG_DT, \
CAST(from_unixtime(unix_timestamp(VHCL.FCTRY_IVNTRY_TRNSFR_DT, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM-dd') AS DATE) FCTRY_IVNTRY_TRNSFR_DT, \
CAST(from_unixtime(unix_timestamp(VHCL.RTL_SL_LSE_DT, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM-dd') AS DATE) RTL_SL_LSE_DT, \
CAST(from_unixtime(unix_timestamp(VHCL.RTL_SL_LSE_DT, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM') AS DATE) RTL_SL_LSE_DT_MNTH_YR, \
VHCL.BDY_STYL_NM, \
VHCL.DRV_TRN_CD , \
VHCL.VHCL_ENGN_MDL_CD,  \
VHCL.VHCL_NMC_MDL_CD, \
VHCL.TRNMSN_TYP_CD,  \
VHCL.EXTR_CLR_CD, \
VHCL.symptm_ds   , \
VHCL.trbl_ds , \
vhcl.trm_lvl_ds, \
vhcl.whlsl_dlr_nb, \
vhcl.vhcl_nmc_mdl_nm, \
vhcl.trnmsn_typ_nm, \
vhcl.rtl_dlr_nb, \
CAST(from_unixtime(unix_timestamp(VHCL.orgnl_whlsl_dt, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM') AS DATE) orgnl_whlsl_dt_MNTH_YR, \
vhcl.orgnl_rtl_st_cd, \
vhcl.orgnl_rtl_dlr_nb, \
CAST(from_unixtime(unix_timestamp(VHCL.ORGNL_IN_SVC_DT, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM') AS DATE) ORGNL_IN_SVC_DT_MNTH_YR, \
from_unixtime(unix_timestamp(VHCL.MNFCTG_DT, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy') MNFCTG_DT_YR, \
from_unixtime(unix_timestamp(VHCL.MNFCTG_DT, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'MM') MNFCTG_DT_MNTH, \
vhcl.ivntry_sts_cd, \
CAST(from_unixtime(unix_timestamp(VHCL.fctry_shp_dt, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM-dd') AS DATE) fctry_shp_dt, \
vhcl.extr_clr_nm, \
vhcl.emsn_crtfct_nb, \
VHCL.wrnty_trbl_cd, \
VHCL.wrnty_symptm_cd, \
VHCL.WRNTY_PFP_NB, \
VHCL.wrnty_clm_sts_cd, \
VHCL.wrnty_aflt_rpt_exclsn_rfrnc_nb, \
VHCL.vhcl_hs_fl_rcrd_typ_cd, \
CAST(VHCL.vhcl_mlg_nb AS STRING), \
VHCL.rcvd_pfp_1st_5_nb, \
VHCL.prt_nm_cmpnt_cd, \
VHCL.fqi_grp_nm, \
CONCAT(VHCL.`drvd_pfp_1st_5_nb`,' - ',VHCL.PFP_DS) DRVD_PFP_DS, \
VHCL.drvd_pfp_1st_5_nb, \
VHCL.dlr_rpr_ordr_nb, \
VHCL.cvrg_clas_cd, \
VHCL.cst_ds, \
VHCL.cst_cmpnnt_ds, \
VHCL.clm_typ_cd, \
VHCL.clm_dstrbr_cd, \
VHCL.wrnty_bsns_typ_cd, \
VHCL.blng_prcs_in, \
VHCL.GLBL_SPLR_NB,  \
VHCL.PLNT_ASGNMT_CD, \
VHCL.WRNTY_CLM_NB,  \
VHCL.FQI_WRNTY_PFP_NB, \
CAST(VHCL.DLR_TOTL_AM AS DECIMAL) DLR_TOTL_AM, \
CAST(VHCL.CLM_NET_AM AS DECIMAL) CLM_NET_AM, \
CAST(VHCL.CLM_RCVRY_AM AS DECIMAL) CLM_RCVRY_AM, \
CAST(VHCL.CLM_DSTRBR_TOTL_AM AS DECIMAL) CLM_DSTRBR_TOTL_AM, \
CAST(from_unixtime(unix_timestamp(VHCL.CLM_DT, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM-dd') AS DATE) CLM_DT, \
CAST(VHCL.CLM_FNCL_DSBRMT_DT_NB AS STRING),  \
CAST(from_unixtime(unix_timestamp(VHCL.VHCL_RPR_DT, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM-dd') AS DATE) VHCL_RPR_DT, \
VHCL.PFP_DS WRNTY_PFP_DS, \
VHCL.svc_dlr_nm  , \
VHCL.svc_dlr_RGN_NM  , \
VHCL.svc_dlr_st_nm , \
VHCL.svc_dlr_PSTL_CD, \
VHCL.svc_dlr_nb, \
VHCL.jd_pwr_cd, \
VHCL.WRNTY_ORGNL_PNC_ID, \
VHCL.PNC_NM, \
VHCL.RCVD_PFP_SPLR_NB, \
VHCL.CNSDTD_PFP_NB,   \
VHCL.SOLD_FLAG, \
from_unixtime(unix_timestamp(VHCL.VHCL_RPR_DT, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM') VHCL_RPR_DT_MNTH_YR, \
from_unixtime(unix_timestamp(VHCL.CLM_DT, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM') CLM_DT_MNTH_YR, \
CASE \
WHEN VHCL.MNFCTG_DT IS NULL THEN  \
NULL \
ELSE  \
floor(months_between(current_date,to_date(CONCAT(year(from_unixtime(unix_timestamp(VHCL.MNFCTG_DT, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM-dd')),'-',month(from_unixtime(unix_timestamp(VHCL.MNFCTG_DT, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM-dd')),'-01')))) \
END \
AS VHCL_AGE_MNTH, \
CASE \
WHEN VHCL.ORGNL_IN_SVC_DT IS NULL \
THEN \
NULL \
ELSE \
GREATEST ( \
cast(0 as bigint), \
floor(datediff(current_date,from_unixtime(unix_timestamp(VHCL.ORGNL_IN_SVC_DT, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM-dd')) / (30.4167)) \
) \
END \
AS VHCL_MIS, \
(CASE WHEN VHCL.orgnl_in_svc_dt IS NULL  THEN NULL \
WHEN VHCL.orgnl_in_svc_dt = cast(to_date(from_unixtime(unix_timestamp('0001-01-01', 'yyyy-MM-dd'))) as date) \
THEN NULL \
WHEN VHCL.vhcl_rpr_dt IS NULL THEN NULL \
WHEN VHCL.vhcl_rpr_dt = cast(to_date(from_unixtime(unix_timestamp('0001-01-01', 'yyyy-MM-dd'))) as date) \
THEN NULL \
WHEN CEIL(DATEDIFF(VHCL.vhcl_rpr_dt, VHCL.orgnl_in_svc_dt)/(30.4167)) <0 THEN 0 \
ELSE CEIL(DATEDIFF(VHCL.vhcl_rpr_dt, VHCL.orgnl_in_svc_dt)/(30.4167)) END) AS CLM_MIS, \
vhcl.EMSN_TYP_CD, \
vhcl.NCI_IVNTRY_STS_CD, \
vhcl.NCI_LCTN_STS_CD, \
vhcl.EIM_CD, \
vhcl.fctry_optns_tx, \
vhcl.gnrc_optns_tx, \
vhcl.VHCL_LN_CD, \
vhcl.NCI_OPTN_GRP_CD, \
vhcl.VHCL_NCI_MDL_CD, \
vhcl.NCI_VHCL_SRS_CD, \
vhcl.NCI_IVNTRY_ADDED_DT, \
vhcl.NMC_RCPT_DT, \
vhcl.ORGNL_RTL_DT, \
vhcl.INVC_DRFT_DT, \
vhcl.orgnl_rtl_dlr_cty_nm, \
vhcl.rtl_dlr_cty_nm, \
vhcl.rtl_dlr_st_cd, \
vhcl.whlsl_dlr_st_cd, \
VHCL.CLM_CVRG_CD, \
VHCL.CVRG_ITM_CD, \
VHCL.WRNTY_BTRY_DGNSTC_CD, \
VHCL.RQSTD_CVRG_CD, \
CAST(VHCL.PRTS_RPLC_DT AS STRING), \
CAST(VHCL.CRDT_SM_RPT_DT AS STRING), \
CAST(VHCL.VHCL_KM_NB AS STRING), \
CAST(VHCL.CLM_WO_OPN_DT AS STRING), \
VHCL.AFLT_DLR_NB, \
VHCL.VHCL_RPR_ST_CD, \
VHCL.SRC_SPLR_ID, \
concat_ws(',',VHCL.prt_nb) as prt_nb, \
VHCL.wrnty_engnr_nm, \
VHCL.rgn_engnr_nm, \
VHCL.tlmtcs_in, \
CASE  \
WHEN VHCL.orgnl_in_svc_dt is NULL \
THEN datediff(from_unixtime(unix_timestamp(),'yyyy-MM-dd') ,from_unixtime(unix_timestamp(vhcl.mnfctg_dt, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM-dd')) \
ELSE datediff(from_unixtime(unix_timestamp(orgnl_in_svc_dt, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM-dd') ,from_unixtime(unix_timestamp(vhcl.mnfctg_dt, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM-dd'))  \
END AS lot_days, \
VHCL.mnfctg_vhcl_plnt_cd, \
VHCL.orgnl_whlsl_dlr_st_cd, \
VHCL.orgnl_whlsl_dt, \
VHCL.cbu_fqi_engnr_nm, \
CAST(VHCL.claims_mod_dt AS STRING), \
VHCL.dlr_pfp_ds, \
VHCL.rspsbl_engnr, \
VHCL.orgnl_lbr_codes, \
VHCL.orgnl_lbr_oprtn_cd_primary_y, \
VHCL.pfp_ds, \
CAST(VHCL.svc_oprtn_prmry_in AS STRING), \
VHCL.trd_cd, \
cast(VHCL.vhcl_lst_rplcd_km_nb as string), \
cast(VHCL.vhcl_lst_rplcd_mlg_nb as string), \
VHCL.vhcl_prdctn_src_cd ,\
vhcl.rgn_nm, \
vhcl.wrnty_pfp_rcvd_splr_desc, \
vhcl.wrnty_pfp_fncl_splr_desc, \
vhcl.wrnty_pfp_rcvd_splr_desc as wrnty_splr_nm, \
CAST(from_unixtime(unix_timestamp(vhcl.crte_tm_stmp, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM-dd') AS DATE) bde_wrnty_add_dt, \
1 as FLAG, \
0 AS VEHICLE_RECORD, \
1 AS CLAIM_RECORD, \
'IR_COMBINED_FULL' AS CRTE_USR_ID, \
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS, \
'IR_COMBINED_FULL' AS UPDT_USR_ID, \
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS \
FROM equip.wrnty_clm_vhcl_solr_view vhcl \
WHERE  \
VHCL.GLBL_MRKT_CD IN ('USA') \
AND (VHCL.VHCL_FLT_IN <> 'Y' OR VHCL.VHCL_FLT_IN IS NULL) \
AND (VHCL.CV_IN NOT IN ('A', 'C') OR VHCL.CV_IN IS NULL) \
AND (VHCL.VHCL_CRSHD_IN <> 'Y' OR VHCL.VHCL_CRSHD_IN IS NULL) \
AND  \
VHCL.vhcl_frgmt_in = 'N' \
AND VHCL.CVRG_CLAS_CD IN ('0', '3', '4', '6', '7','8') \
AND VHCL.WRNTY_ORGNL_PNC_ID NOT IN ('CCG00','1A001') \
AND VHCL.WRNTY_CLM_STS_CD NOT IN ('ER') \
AND VHCL.CLM_DSTRBR_CD IN ('307', '303') \
AND VHCL.BLNG_PRCS_IN = 'B' \
AND VHCL.SVC_DLR_NB NOT IN \
( \
'AD529', \
'AD530', \
'7A084', \
'7A085', \
'7I001', \
'7I002', \
'7I003', \
'7I004', \
'7I007', \
'7I008', \
'TA002', \
'TA003', \
'TA004', \
'TA005', \
'TA006', \
'TA018', \
'TA023', \
'TA026') \
AND VHCL.rpt_exclsn_in NOT IN ('X', 'A', 'C', 'V') \
UNION ALL \
SELECT  \
vin_vhcl.vin_id VIN_ID,  \
vin_vhcl.EMSN_TYP_NM, \
vin_vhcl.ENGN_PRFX_8_DGT_CD, \
vin_vhcl.PRDCTN_MDL_SRS_CD, \
vin_vhcl.NHTSA_VHCL_TYP_CD, \
CASE WHEN TRIM(vin_vhcl.vhcl_mk_cd) = 'N' THEN 'Nissan' WHEN  TRIM(vin_vhcl.vhcl_mk_cd) = 'I' THEN 'Infiniti' ELSE '' END vhcl_mk_nm,   \
vin_vhcl.GLBL_MRKT_CD, \
vin_vhcl.VHCL_FLT_IN, \
vin_vhcl.CV_IN, \
vin_vhcl.VHCL_CRSHD_IN, \
'VHCL' RPT_EXCLSN_IN, \
vin_vhcl.VHCL_LN_NM,  \
vhcl_yr_nb, \
vin_vhcl.NML_PRDCTN_MDL_CD,    \
upper(plnt_cd_nm) as plnt_cd_nm, \
CAST(from_unixtime(unix_timestamp(vin_vhcl.ORGNL_IN_SVC_DT, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM-dd') AS DATE) ORGNL_IN_SVC_DT, \
CAST(from_unixtime(unix_timestamp(vin_vhcl.IN_SVC_DT, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM-dd') AS DATE) IN_SVC_DT, \
from_unixtime(unix_timestamp(vin_vhcl.MNFCTG_DT, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM') MNFCTG_DT_MNTH_YR, \
CAST(from_unixtime(unix_timestamp(vin_vhcl.MNFCTG_DT, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM-dd') AS DATE) MNFCTG_DT, \
CAST(from_unixtime(unix_timestamp(vin_vhcl.FCTRY_IVNTRY_TRNSFR_DT, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM-dd') AS DATE) FCTRY_IVNTRY_TRNSFR_DT, \
CAST(from_unixtime(unix_timestamp(vin_vhcl.RTL_SL_LSE_DT, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM-dd') AS DATE) RTL_SL_LSE_DT, \
CAST(from_unixtime(unix_timestamp(vin_VHCL.RTL_SL_LSE_DT, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM') AS DATE) RTL_SL_LSE_DT_MNTH_YR, \
vin_vhcl.BDY_STYL_NM, \
vin_vhcl.DRV_TRN_CD , \
vin_vhcl.VHCL_ENGN_MDL_CD,  \
vin_vhcl.VHCL_NMC_MDL_CD, \
vin_vhcl.TRNMSN_TYP_CD,  \
vin_vhcl.EXTR_CLR_CD, \
'VHCL' symptm_ds   , \
'VHCL' trbl_ds , \
vin_vhcl.trm_lvl_ds, \
vin_vhcl.whlsl_dlr_nb, \
vin_vhcl.vhcl_nmc_mdl_nm, \
vin_vhcl.trnmsn_typ_nm, \
vin_vhcl.rtl_dlr_nb, \
CAST(from_unixtime(unix_timestamp(vin_VHCL.orgnl_whlsl_dt, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM') AS DATE) orgnl_whlsl_dt_MNTH_YR, \
vin_vhcl.orgnl_rtl_st_cd, \
vin_vhcl.orgnl_rtl_dlr_nb, \
CAST(from_unixtime(unix_timestamp(vin_VHCL.ORGNL_IN_SVC_DT, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM') AS DATE) ORGNL_IN_SVC_DT_MNTH_YR, \
from_unixtime(unix_timestamp(vin_VHCL.MNFCTG_DT, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy') MNFCTG_DT_YR, \
from_unixtime(unix_timestamp(vin_VHCL.MNFCTG_DT, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'MM') MNFCTG_DT_MNTH, \
vin_vhcl.ivntry_sts_cd, \
CAST(from_unixtime(unix_timestamp(vin_vhcl.fctry_shp_dt, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM-dd') AS DATE) fctry_shp_dt, \
vin_vhcl.extr_clr_nm, \
vin_vhcl.emsn_crtfct_nb, \
'VHCL' wrnty_trbl_cd, \
'VHCL' wrnty_symptm_cd, \
'VHCL' WRNTY_PFP_NB, \
'VHCL' wrnty_clm_sts_cd, \
'VHCL' wrnty_aflt_rpt_exclsn_rfrnc_nb, \
'VHCL' vhcl_hs_fl_rcrd_typ_cd, \
'VHCL' vhcl_mlg_nb, \
'VHCL' rcvd_pfp_1st_5_nb, \
'VHCL' prt_nm_cmpnt_cd, \
'VHCL' fqi_grp_nm, \
'VHCL' DRVD_PFP_DS, \
'VHCL' drvd_pfp_1st_5_nb, \
'VHCL' dlr_rpr_ordr_nb, \
'VHCL' cvrg_clas_cd, \
'VHCL' cst_ds, \
'VHCL' cst_cmpnnt_ds, \
'VHCL' clm_typ_cd, \
'VHCL' clm_dstrbr_cd, \
'VHCL' wrnty_bsns_typ_cd, \
'VHCL' blng_prcs_in, \
'VHCL' GLBL_SPLR_NB,  \
'VHCL' PLNT_ASGNMT_CD, \
'VHCL' WRNTY_CLM_NB,  \
'VHCL' FQI_WRNTY_PFP_NB, \
0 DLR_TOTL_AM, \
0 CLM_NET_AM, \
0 CLM_RCVRY_AM, \
0 CLM_DSTRBR_TOTL_AM, \
'VHCL' CLM_DT, \
'VHCL' CLM_FNCL_DSBRMT_DT_NB,  \
'VHCL' VHCL_RPR_DT, \
'VHCL' WRNTY_PFP_DS, \
'VHCL' dlr_nm  , \
'VHCL' DLR_RGN_NM  , \
'VHCL' dlr_st_nm , \
'VHCL' DLR_PSTL_CD, \
'VHCL' svc_dlr_nb, \
'VHCL' jd_pwr_cd, \
'VHCL' WRNTY_ORGNL_PNC_ID, \
'VHCL' PNC_NM, \
'VHCL' RCVD_PFP_SPLR_NB, \
'VHCL' CNSDTD_PFP_NB,   \
vin_vhcl.SOLD_FLAG, \
'VHCL' VHCL_RPR_DT_MNTH_YR, \
'VHCL' CLM_DT_MNTH_YR, \
CASE \
WHEN vin_vhcl.MNFCTG_DT IS NULL THEN  \
NULL \
ELSE  \
floor(months_between(current_date,to_date(CONCAT(year(from_unixtime(unix_timestamp(vin_vhcl.MNFCTG_DT, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM-dd')),'-',month(from_unixtime(unix_timestamp(vin_vhcl.MNFCTG_DT, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM-dd')),'-01')))) \
END \
AS VHCL_AGE_MNTH, \
CASE \
WHEN vin_vhcl.ORGNL_IN_SVC_DT IS NULL \
THEN \
NULL \
ELSE \
GREATEST ( \
cast(0 as bigint), \
floor(datediff(current_date,from_unixtime(unix_timestamp(vin_vhcl.ORGNL_IN_SVC_DT, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM-dd')) / (30.4167)) \
) \
END \
AS VHCL_MIS, \
'VHCL' CLM_MIS, \
vin_vhcl.EMSN_TYP_CD, \
vin_vhcl.NCI_IVNTRY_STS_CD, \
vin_vhcl.NCI_LCTN_STS_CD, \
vin_vhcl.EIM_CD, \
vin_vhcl.fctry_optns_tx, \
vin_vhcl.gnrc_optns_tx, \
vin_vhcl.VHCL_LN_CD, \
vin_vhcl.NCI_OPTN_GRP_CD, \
vin_vhcl.VHCL_NCI_MDL_CD, \
vin_vhcl.NCI_VHCL_SRS_CD, \
vin_vhcl.NCI_IVNTRY_ADDED_DT, \
vin_vhcl.NMC_RCPT_DT, \
vin_vhcl.ORGNL_RTL_DT, \
vin_vhcl.INVC_DRFT_DT, \
vin_vhcl.orgnl_rtl_dlr_cty_nm, \
vin_vhcl.rtl_dlr_cty_nm, \
vin_vhcl.rtl_dlr_st_cd, \
vin_vhcl.whlsl_dlr_st_cd, \
'VHCL' CLM_CVRG_CD, \
'VHCL' CVRG_ITM_CD, \
'VHCL' WRNTY_BTRY_DGNSTC_CD, \
'VHCL' RQSTD_CVRG_CD, \
'VHCL' PRTS_RPLC_DT, \
'VHCL' CRDT_SM_RPT_DT, \
'VHCL' VHCL_KM_NB, \
'VHCL' CLM_WO_OPN_DT, \
'VHCL' AFLT_DLR_NB, \
'VHCL' VHCL_RPR_ST_CD, \
'VHCL' SRC_SPLR_ID, \
'VHCL' prt_nb, \
'VHCL' wrnty_engnr_nm, \
'VHCL' rgn_engnr_nm, \
vin_vhcl.tlmtcs_ind as tlmtcs_in, \
CASE  \
WHEN vin_vhcl.orgnl_in_svc_dt is NULL \
THEN datediff(from_unixtime(unix_timestamp(),'yyyy-MM-dd') ,from_unixtime(unix_timestamp(vin_vhcl.mnfctg_dt, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM-dd')) \
ELSE datediff(from_unixtime(unix_timestamp(orgnl_in_svc_dt, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM-dd') ,from_unixtime(unix_timestamp(vin_vhcl.mnfctg_dt, 'MM/dd/yyyy HH:mm:ss.SSSSSS'),'yyyy-MM-dd'))  \
END AS lot_days, \
vin_vhcl.mnfctg_vhcl_plnt_cd, \
vin_vhcl.orgnl_whlsl_dlr_st_cd, \
vin_vhcl.orgnl_whlsl_dt, \
'VHCL' cbu_fqi_engnr_nm, \
'VHCL' claims_mod_dt, \
'VHCL' dlr_pfp_ds, \
'VHCL' rspsbl_engnr, \
'VHCL' orgnl_lbr_codes, \
'VHCL' orgnl_lbr_oprtn_cd_primary_y, \
'VHCL' pfp_ds, \
'VHCL' svc_oprtn_prmry_in, \
'VHCL' trd_cd, \
'VHCL' vhcl_lst_rplcd_km_nb, \
'VHCL' vhcl_lst_rplcd_mlg_nb, \
vin_vhcl.vhcl_prdctn_src_cd , \
'VHCL' rgn_nm, \
'VHCL' wrnty_pfp_rcvd_splr_desc, \
'VHCL' wrnty_pfp_fncl_splr_desc, \
'VHCL' wrnty_splr_nm, \
'VHCL' bde_wrnty_add_dt, \
1 as FLAG, \
1 AS VEHICLE_RECORD, \
0 AS CLAIM_RECORD, \
'IR_COMBINED_FULL' AS CRTE_USR_ID, \
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS, \
'IR_COMBINED_FULL' AS UPDT_USR_ID, \
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS \
FROM equip.vhcl_solr vin_vhcl \
WHERE  \
vin_vhcl.GLBL_MRKT_CD IN ('USA') \
AND (vin_vhcl.VHCL_FLT_IN <> 'Y' OR vin_vhcl.VHCL_FLT_IN IS NULL) \
AND (vin_vhcl.CV_IN NOT IN ('A', 'C') OR vin_vhcl.CV_IN IS NULL) \
AND (vin_vhcl.VHCL_CRSHD_IN <> 'Y' OR vin_vhcl.VHCL_CRSHD_IN IS NULL) \
AND  \
vin_vhcl.vhcl_frgmt_in = 'N'")
