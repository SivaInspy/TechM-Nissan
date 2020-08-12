#########################################################################################################################################
#   																																	#
#   Created by          : Nandha Kumaran M marimn1																								#
#   Last Updated by     : Sivakumar D x135756																						#
#   Last Updated date   : 07/28/2020  																									#
#   Script Name         : create_and_populate_ir_combined_limited_set_vmis_cmis.py  															#
#   Description         : Full load of ir_cmbnd_vhcl_clm_lmtd_flds_vms_cms																	#
#   Log file path       : /projects/equip/logs                                                                                                                     #
#   																																	#
#########################################################################################################################################

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import sys, time

#initalize SparkSession

hq = SparkSession.builder.appName("ir_cmbnd_vhcl_clm_lmtd_flds_vms_cms").enableHiveSupport().getOrCreate()

hq.sql("DROP TABLE IF EXISTS equip.ir_cmbnd_vhcl_clm_lmtd_flds_vms_cms")

hq.sql("CREATE TABLE equip.ir_cmbnd_vhcl_clm_lmtd_flds_vms_cms \
STORED AS ORC  \
AS \
SELECT  \
VIN_ID,  \
vhcl_mk_nm,   \
GLBL_MRKT_CD, \
VHCL_FLT_IN, \
CV_IN, \
VHCL_CRSHD_IN, \
RPT_EXCLSN_IN, \
VHCL_LN_NM,  \
vhcl_yr_nb, \
NML_PRDCTN_MDL_CD,    \
upper(plnt_cd_nm) as plnt_cd_nm, \
ORGNL_IN_SVC_DT, \
IN_SVC_DT, \
MNFCTG_DT_MNTH_YR, \
MNFCTG_DT, \
FCTRY_IVNTRY_TRNSFR_DT, \
RTL_SL_LSE_DT, \
BDY_STYL_NM, \
DRV_TRN_CD , \
VHCL_ENGN_MDL_CD,  \
VHCL_NMC_MDL_CD, \
TRNMSN_TYP_CD,  \
EXTR_CLR_CD, \
symptm_ds   , \
trbl_ds                 , \
trm_lvl_ds, \
EMSN_TYP_NM, \
ENGN_PRFX_8_DGT_CD, \
PRDCTN_MDL_SRS_CD, \
NHTSA_VHCL_TYP_CD, \
whlsl_dlr_nb, \
vhcl_nmc_mdl_nm, \
trnmsn_typ_nm, \
RTL_SL_LSE_DT_MNTH_YR, \
rtl_dlr_nb, \
orgnl_whlsl_dt_MNTH_YR, \
orgnl_rtl_st_cd, \
orgnl_rtl_dlr_nb, \
ORGNL_IN_SVC_DT_MNTH_YR, \
MNFCTG_DT_YR, \
MNFCTG_DT_MNTH, \
ivntry_sts_cd, \
fctry_shp_dt, \
extr_clr_nm, \
emsn_crtfct_nb, \
wrnty_trbl_cd, \
wrnty_symptm_cd, \
WRNTY_PFP_NB, \
wrnty_clm_sts_cd, \
wrnty_aflt_rpt_exclsn_rfrnc_nb, \
vhcl_hs_fl_rcrd_typ_cd, \
vhcl_mlg_nb, \
rcvd_pfp_1st_5_nb, \
prt_nm_cmpnt_cd, \
fqi_grp_nm, \
DRVD_PFP_DS, \
drvd_pfp_1st_5_nb, \
dlr_rpr_ordr_nb, \
cvrg_clas_cd, \
cst_ds, \
cst_cmpnnt_ds, \
clm_typ_cd, \
clm_dstrbr_cd, \
wrnty_bsns_typ_cd, \
blng_prcs_in, \
GLBL_SPLR_NB,  \
PLNT_ASGNMT_CD, \
WRNTY_CLM_NB,                      \
FQI_WRNTY_PFP_NB, \
DLR_TOTL_AM, \
CLM_NET_AM, \
CLM_RCVRY_AM, \
CLM_DSTRBR_TOTL_AM, \
CLM_DT,     \
CLM_FNCL_DSBRMT_DT_NB,  \
VHCL_RPR_DT, \
WRNTY_PFP_DS, \
svc_dlr_nm                  , \
svc_DLR_RGN_NM                  , \
svc_dlr_st_nm             , \
svc_dlr_nb, \
svc_DLR_PSTL_CD, \
jd_pwr_cd, \
WRNTY_ORGNL_PNC_ID, \
PNC_NM, \
RCVD_PFP_SPLR_NB, \
CNSDTD_PFP_NB,   \
SOLD_FLAG, \
VHCL_RPR_DT_MNTH_YR, \
CLM_DT_MNTH_YR, \
VHCL_AGE_MNTH, \
VHCL_MIS, \
CLM_MIS, \
FLAG, \
VEHICLE_RECORD, \
CLAIM_RECORD, \
EMSN_TYP_CD, \
NCI_IVNTRY_STS_CD, \
NCI_LCTN_STS_CD, \
EIM_CD, \
fctry_optns_tx, \
gnrc_optns_tx, \
VHCL_LN_CD, \
NCI_OPTN_GRP_CD, \
VHCL_NCI_MDL_CD, \
NCI_VHCL_SRS_CD, \
NCI_IVNTRY_ADDED_DT, \
NMC_RCPT_DT, \
ORGNL_RTL_DT, \
INVC_DRFT_DT, \
orgnl_rtl_dlr_cty_nm, \
rtl_dlr_cty_nm, \
rtl_dlr_st_cd, \
whlsl_dlr_st_cd, \
CLM_CVRG_CD, \
CVRG_ITM_CD, \
WRNTY_BTRY_DGNSTC_CD, \
RQSTD_CVRG_CD, \
PRTS_RPLC_DT, \
CRDT_SM_RPT_DT, \
VHCL_KM_NB, \
CLM_WO_OPN_DT, \
AFLT_DLR_NB, \
VHCL_RPR_ST_CD, \
SRC_SPLR_ID, \
prt_nb, \
wrnty_engnr_nm, \
rgn_engnr_nm, \
tlmtcs_in, \
lot_days, \
mnfctg_vhcl_plnt_cd, \
orgnl_whlsl_dlr_st_cd, \
orgnl_whlsl_dt, \
cbu_fqi_engnr_nm, \
claims_mod_dt, \
dlr_pfp_ds, \
rspsbl_engnr, \
orgnl_lbr_codes, \
orgnl_lbr_oprtn_cd_primary_y, \
pfp_ds, \
svc_oprtn_prmry_in, \
trd_cd, \
vhcl_lst_rplcd_km_nb, \
vhcl_lst_rplcd_mlg_nb, \
vhcl_prdctn_src_cd ,\
wrnty_pfp_rcvd_splr_desc, \
rgn_nm, \
wrnty_pfp_fncl_splr_desc, \
wrnty_splr_nm, \
bde_wrnty_add_dt, \
CASE WHEN VHCL_MIS >= 0 THEN 1 ELSE 0 END V_MIS_0, \
CASE WHEN VHCL_MIS >= 1 THEN 1 ELSE 0 END V_MIS_1, \
CASE WHEN VHCL_MIS >= 2 THEN 1 ELSE 0 END V_MIS_2, \
CASE WHEN VHCL_MIS >= 3 THEN 1 ELSE 0 END V_MIS_3, \
CASE WHEN VHCL_MIS >= 4 THEN 1 ELSE 0 END V_MIS_4, \
CASE WHEN VHCL_MIS >= 5 THEN 1 ELSE 0 END V_MIS_5, \
CASE WHEN VHCL_MIS >= 6 THEN 1 ELSE 0 END V_MIS_6, \
CASE WHEN VHCL_MIS >= 7 THEN 1 ELSE 0 END V_MIS_7, \
CASE WHEN VHCL_MIS >= 8 THEN 1 ELSE 0 END V_MIS_8, \
CASE WHEN VHCL_MIS >= 9 THEN 1 ELSE 0 END V_MIS_9, \
CASE WHEN VHCL_MIS >= 10 THEN 1 ELSE 0 END V_MIS_10, \
CASE WHEN VHCL_MIS >= 11 THEN 1 ELSE 0 END V_MIS_11, \
CASE WHEN VHCL_MIS >= 12 THEN 1 ELSE 0 END V_MIS_12, \
CASE WHEN VHCL_MIS >= 13 THEN 1 ELSE 0 END V_MIS_13, \
CASE WHEN VHCL_MIS >= 14 THEN 1 ELSE 0 END V_MIS_14, \
CASE WHEN VHCL_MIS >= 15 THEN 1 ELSE 0 END V_MIS_15, \
CASE WHEN VHCL_MIS >= 16 THEN 1 ELSE 0 END V_MIS_16, \
CASE WHEN VHCL_MIS >= 17 THEN 1 ELSE 0 END V_MIS_17, \
CASE WHEN VHCL_MIS >= 18 THEN 1 ELSE 0 END V_MIS_18, \
CASE WHEN VHCL_MIS >= 19 THEN 1 ELSE 0 END V_MIS_19, \
CASE WHEN VHCL_MIS >= 20 THEN 1 ELSE 0 END V_MIS_20, \
CASE WHEN VHCL_MIS >= 21 THEN 1 ELSE 0 END V_MIS_21, \
CASE WHEN VHCL_MIS >= 22 THEN 1 ELSE 0 END V_MIS_22, \
CASE WHEN VHCL_MIS >= 23 THEN 1 ELSE 0 END V_MIS_23, \
CASE WHEN VHCL_MIS >= 24 THEN 1 ELSE 0 END V_MIS_24, \
CASE WHEN VHCL_MIS >= 25 THEN 1 ELSE 0 END V_MIS_25, \
CASE WHEN VHCL_MIS >= 26 THEN 1 ELSE 0 END V_MIS_26, \
CASE WHEN VHCL_MIS >= 27 THEN 1 ELSE 0 END V_MIS_27, \
CASE WHEN VHCL_MIS >= 28 THEN 1 ELSE 0 END V_MIS_28, \
CASE WHEN VHCL_MIS >= 29 THEN 1 ELSE 0 END V_MIS_29, \
CASE WHEN VHCL_MIS >= 30 THEN 1 ELSE 0 END V_MIS_30, \
CASE WHEN VHCL_MIS >= 31 THEN 1 ELSE 0 END V_MIS_31, \
CASE WHEN VHCL_MIS >= 32 THEN 1 ELSE 0 END V_MIS_32, \
CASE WHEN VHCL_MIS >= 33 THEN 1 ELSE 0 END V_MIS_33, \
CASE WHEN VHCL_MIS >= 34 THEN 1 ELSE 0 END V_MIS_34, \
CASE WHEN VHCL_MIS >= 35 THEN 1 ELSE 0 END V_MIS_35, \
CASE WHEN VHCL_MIS >= 36 THEN 1 ELSE 0 END V_MIS_36, \
CASE WHEN VHCL_MIS >= 37 THEN 1 ELSE 0 END V_MIS_37, \
CASE WHEN VHCL_MIS >= 38 THEN 1 ELSE 0 END V_MIS_38, \
CASE WHEN VHCL_MIS >= 39 THEN 1 ELSE 0 END V_MIS_39, \
CASE WHEN VHCL_MIS >= 40 THEN 1 ELSE 0 END V_MIS_40, \
CASE WHEN VHCL_MIS >= 41 THEN 1 ELSE 0 END V_MIS_41, \
CASE WHEN VHCL_MIS >= 42 THEN 1 ELSE 0 END V_MIS_42, \
CASE WHEN VHCL_MIS >= 43 THEN 1 ELSE 0 END V_MIS_43, \
CASE WHEN VHCL_MIS >= 44 THEN 1 ELSE 0 END V_MIS_44, \
CASE WHEN VHCL_MIS >= 45 THEN 1 ELSE 0 END V_MIS_45, \
CASE WHEN VHCL_MIS >= 46 THEN 1 ELSE 0 END V_MIS_46, \
CASE WHEN VHCL_MIS >= 47 THEN 1 ELSE 0 END V_MIS_47, \
CASE WHEN VHCL_MIS >= 48 THEN 1 ELSE 0 END V_MIS_48, \
CASE WHEN VHCL_MIS >= 49 THEN 1 ELSE 0 END V_MIS_49, \
CASE WHEN VHCL_MIS >= 50 THEN 1 ELSE 0 END V_MIS_50, \
CASE WHEN VHCL_MIS >= 51 THEN 1 ELSE 0 END V_MIS_51, \
CASE WHEN VHCL_MIS >= 52 THEN 1 ELSE 0 END V_MIS_52, \
CASE WHEN VHCL_MIS >= 53 THEN 1 ELSE 0 END V_MIS_53, \
CASE WHEN VHCL_MIS >= 54 THEN 1 ELSE 0 END V_MIS_54, \
CASE WHEN VHCL_MIS >= 55 THEN 1 ELSE 0 END V_MIS_55, \
CASE WHEN VHCL_MIS >= 56 THEN 1 ELSE 0 END V_MIS_56, \
CASE WHEN VHCL_MIS >= 57 THEN 1 ELSE 0 END V_MIS_57, \
CASE WHEN VHCL_MIS >= 58 THEN 1 ELSE 0 END V_MIS_58, \
CASE WHEN VHCL_MIS >= 59 THEN 1 ELSE 0 END V_MIS_59, \
CASE WHEN VHCL_MIS >= 60 THEN 1 ELSE 0 END V_MIS_60, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 0 AND VHCL_MIS>=0 THEN 1 ELSE 0 END C_MIS_0, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 1 AND VHCL_MIS>=1 THEN 1 ELSE 0 END C_MIS_1, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 2 AND VHCL_MIS>=2 THEN 1 ELSE 0 END C_MIS_2, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 3 AND VHCL_MIS>=3 THEN 1 ELSE 0 END C_MIS_3, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 4 AND VHCL_MIS>=4 THEN 1 ELSE 0 END C_MIS_4, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 5 AND VHCL_MIS>=5 THEN 1 ELSE 0 END C_MIS_5, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 6 AND VHCL_MIS>=6 THEN 1 ELSE 0 END C_MIS_6, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 7 AND VHCL_MIS>=7 THEN 1 ELSE 0 END C_MIS_7, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 8 AND VHCL_MIS>=8 THEN 1 ELSE 0 END C_MIS_8, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 9 AND VHCL_MIS>=9 THEN 1 ELSE 0 END C_MIS_9, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 10 AND VHCL_MIS>=10 THEN 1 ELSE 0 END C_MIS_10, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 11 AND VHCL_MIS>=11 THEN 1 ELSE 0 END C_MIS_11, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 12 AND VHCL_MIS>=12 THEN 1 ELSE 0 END C_MIS_12, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 13 AND VHCL_MIS>=13 THEN 1 ELSE 0 END C_MIS_13, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 14 AND VHCL_MIS>=14 THEN 1 ELSE 0 END C_MIS_14, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 15 AND VHCL_MIS>=15 THEN 1 ELSE 0 END C_MIS_15, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 16 AND VHCL_MIS>=16 THEN 1 ELSE 0 END C_MIS_16, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 17 AND VHCL_MIS>=17 THEN 1 ELSE 0 END C_MIS_17, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 18 AND VHCL_MIS>=18 THEN 1 ELSE 0 END C_MIS_18, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 19 AND VHCL_MIS>=19 THEN 1 ELSE 0 END C_MIS_19, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 20 AND VHCL_MIS>=20 THEN 1 ELSE 0 END C_MIS_20, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 21 AND VHCL_MIS>=21 THEN 1 ELSE 0 END C_MIS_21, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 22 AND VHCL_MIS>=22 THEN 1 ELSE 0 END C_MIS_22, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 23 AND VHCL_MIS>=23 THEN 1 ELSE 0 END C_MIS_23, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 24 AND VHCL_MIS>=24 THEN 1 ELSE 0 END C_MIS_24, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 25 AND VHCL_MIS>=25 THEN 1 ELSE 0 END C_MIS_25, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 26 AND VHCL_MIS>=26 THEN 1 ELSE 0 END C_MIS_26, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 27 AND VHCL_MIS>=27 THEN 1 ELSE 0 END C_MIS_27, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 28 AND VHCL_MIS>=28 THEN 1 ELSE 0 END C_MIS_28, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 29 AND VHCL_MIS>=29 THEN 1 ELSE 0 END C_MIS_29, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 30 AND VHCL_MIS>=30 THEN 1 ELSE 0 END C_MIS_30, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 31 AND VHCL_MIS>=31 THEN 1 ELSE 0 END C_MIS_31, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 32 AND VHCL_MIS>=32 THEN 1 ELSE 0 END C_MIS_32, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 33 AND VHCL_MIS>=33 THEN 1 ELSE 0 END C_MIS_33, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 34 AND VHCL_MIS>=34 THEN 1 ELSE 0 END C_MIS_34, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 35 AND VHCL_MIS>=35 THEN 1 ELSE 0 END C_MIS_35, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 36 AND VHCL_MIS>=36 THEN 1 ELSE 0 END C_MIS_36, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 37 AND VHCL_MIS>=37 THEN 1 ELSE 0 END C_MIS_37, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 38 AND VHCL_MIS>=38 THEN 1 ELSE 0 END C_MIS_38, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 39 AND VHCL_MIS>=39 THEN 1 ELSE 0 END C_MIS_39, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 40 AND VHCL_MIS>=40 THEN 1 ELSE 0 END C_MIS_40, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 41 AND VHCL_MIS>=41 THEN 1 ELSE 0 END C_MIS_41, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 42 AND VHCL_MIS>=42 THEN 1 ELSE 0 END C_MIS_42, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 43 AND VHCL_MIS>=43 THEN 1 ELSE 0 END C_MIS_43, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 44 AND VHCL_MIS>=44 THEN 1 ELSE 0 END C_MIS_44, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 45 AND VHCL_MIS>=45 THEN 1 ELSE 0 END C_MIS_45, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 46 AND VHCL_MIS>=46 THEN 1 ELSE 0 END C_MIS_46, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 47 AND VHCL_MIS>=47 THEN 1 ELSE 0 END C_MIS_47, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 48 AND VHCL_MIS>=48 THEN 1 ELSE 0 END C_MIS_48, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 49 AND VHCL_MIS>=49 THEN 1 ELSE 0 END C_MIS_49, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 50 AND VHCL_MIS>=50 THEN 1 ELSE 0 END C_MIS_50, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 51 AND VHCL_MIS>=51 THEN 1 ELSE 0 END C_MIS_51, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 52 AND VHCL_MIS>=52 THEN 1 ELSE 0 END C_MIS_52, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 53 AND VHCL_MIS>=53 THEN 1 ELSE 0 END C_MIS_53, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 54 AND VHCL_MIS>=54 THEN 1 ELSE 0 END C_MIS_54, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 55 AND VHCL_MIS>=55 THEN 1 ELSE 0 END C_MIS_55, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 56 AND VHCL_MIS>=56 THEN 1 ELSE 0 END C_MIS_56, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 57 AND VHCL_MIS>=57 THEN 1 ELSE 0 END C_MIS_57, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 58 AND VHCL_MIS>=58 THEN 1 ELSE 0 END C_MIS_58, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 59 AND VHCL_MIS>=59 THEN 1 ELSE 0 END C_MIS_59, \
CASE WHEN CLAIM_RECORD = 1 AND CLM_MIS = 60 AND VHCL_MIS>=60 THEN 1 ELSE 0 END C_MIS_60, \
'IR_COMBINED_MIS_FULL' AS CRTE_USR_ID, \
cast(from_unixtime(unix_timestamp()) as timestamp) AS CRTE_TS, \
'IR_COMBINED_MIS_FULL' AS UPDT_USR_ID, \
cast(from_unixtime(unix_timestamp()) as timestamp) AS UPDT_TS \
FROM equip.ir_cmbnd_vhcl_clm_lmtd_flds")
