sqoop import --connect jdbc:oracle:thin:@10.78.79.27:55298/OPDBEFLO --query "select INCDNT_IVSTGN_RPT_FORM_NM,TRNSCN_DT,ESTMTD_SPD_TX,OTHR_VHCL_INVLV_IN,PRPRTY_DS,TRD_EFCTV_DT,VHCL_ISPCTN_DT,VHCL_ID_NB,VHCL_MK_NM,VHCL_YR_NB,VHCL_NMC_MDL_CD,CLMNT_ST_CD,CLMNT_ST_NM,CLMNT_CNTRY_CD,ADTNL_OBSVTN_TX,VHCL_ADVNCD_TCHLGY_IN,ARBG_OBSVTN_TX,ALGTN_CMPNT_LVL_1_NM,ALGTN_CMPNT_LVL_2_NM,ALGTN_CMPNT_LVL_3_NM,BRAK_SPLMNT_IN,SLSFRC_CASE_ID,CTR_REAR_VHCL_OCPNT_INJRY_IN,INCDNT_OCRNC_TS,VHCL_DRVR_INJRY_DS,CTR_REAR_VHCL_OCPNT_INJRY_DS,LFT_REAR_VHCL_OCPNT_INJRY_DS,OTHR_VHCL_OCPNT_INJRY_DS,RGHT_REAR_VHCL_OCPNT_INJRY_DS,ALGTN_DTL_DS,PRDT_SFTY_RSPNS_DS,DRVR_ARBG_STS_NM,VHCL_DRVR_INJRY_IN,PWRTRN_OBSVTN_TX,EXTR_BDY_INCDNT_TX,INCDNT_IVSTGN_RPT_FORM_ID,RMVD_ALGD_DFCTV_PRT_TX,INTRR_INFRMN_TX,LFT_REAR_ARBG_STS_NM,LFT_REAR_VHCL_OCPNT_INJRY_IN,VHCL_PRKD_DRTN_TX,INCDNT_LCTN_TX,MN_ALGTN_NM,ISPCTN_MLG_NB,PRDT_SFTY_MNTR_IN,PRPRTY_DMG_DS,OTHR_VHCL_DMG_DS,PRSN_INVLVD_NB,OTHR_SPLMNT_RPT_NM,ADTNL_VHCL_INFRMN_TX,OTHR_VHCL_OCPNT_INJRY_IN,OTHR_ARBG_STS_NM,OTHR_VHCL_TYP_TX,VHCL_CRNT_LCTN_TX,INCDNT_SHRT_DS,PRDT_SFTY_RSPNS_TX,RSTRNT_SYSTM_SPLMNT_RPT_IN,RGHT_FRNT_VHCL_OCPNT_INJRY_DS,RGHT_FRNT_ARBG_STS_NM,RGHT_FRNT_VHCL_OCPNT_INJRY_IN,RGHT_REAR_ARBG_STS_NM,RGHT_REAR_VHCL_OCPNT_INJRY_IN,SEAT_RSTRNT_OBSVTN_TX,SCNDRY_ALGTN_NM,STERNG_SPLMNT_RPT_IN,THRML_SPLMNT_RPT_IN,TRNMSN_SPLMNT_RPT_IN,UINTD_ACLRT_SPLMNT_RPT_IN,UNDR_CARG_OBSVTN_TX,ESTMTD_VHCL_SPD_TX,IVSTGN_VIN_ID,WTHR_CNDTN_TX,INCDNT_IVSTGN_RPT_PDF_URL_TX,INCDNT_DTL_DS,CSTMR_RQST_TX,INCDNT_VHCL_MDL_YR_NB,INCDNT_VHCL_NMC_MDL_CD,INCDNT_VHCL_MK_NM,ATCHMT_IN,VHCL_NMC_MDL_NM,DRV_TRN_DS,DRV_TRN_CD,TRM_LVL_DS,MNFCR_DT,NML_PRDCTN_MDL_CD,ORGNL_IN_SVC_DT,TRNMSN_TYP_CD,TRNMSN_TYP_NM, EMSN_CRFCTN_NB,CURRENT_TIMESTAMP as CRTE_TS,CAST(' x987731' as VARCHAR(10)) as CRTE_USR_ID,UPDT_TS from (select ffi.INCDNT_IVSTGN_RPT_FORM_NM,
dt.CLNDR_DT AS TRNSCN_DT,
ffi.ESTMTD_SPD_TX,
ffi.OTHR_VHCL_INVLV_IN,
ffi.PRPRTY_DS,
DET.CLNDR_DT AS TRD_EFCTV_DT,
DIV.CLNDR_DT AS VHCL_ISPCTN_DT,
dv.VHCL_ID_NB,
dv.VHCL_MK_NM,
dv.VHCL_YR_NB,
dv.VHCL_NMC_MDL_CD,
ds.st_cd AS CLMNT_ST_CD,
ds.st_nm AS CLMNT_ST_NM,
DC.CNTRY_CD AS CLMNT_CNTRY_CD,
ffi.ADTNL_OBSVTN_TX,
ffi.VHCL_ADVNCD_TCHLGY_IN,
ffi.ARBG_OBSVTN_TX,
ffi.ALGTN_CMPNT_LVL_1_NM,
ffi.ALGTN_CMPNT_LVL_2_NM,
ffi.ALGTN_CMPNT_LVL_3_NM,
ffi.BRAK_SPLMNT_IN,
ffi.SLSFRC_CASE_ID,
ffi.CTR_REAR_VHCL_OCPNT_INJRY_IN,
ffi.INCDNT_OCRNC_TS,
ffi.VHCL_DRVR_INJRY_DS,
ffi.CTR_REAR_VHCL_OCPNT_INJRY_DS,
ffi.LFT_REAR_VHCL_OCPNT_INJRY_DS,
ffi.OTHR_VHCL_OCPNT_INJRY_DS,
ffi.RGHT_REAR_VHCL_OCPNT_INJRY_DS,
ffi.ALGTN_DTL_DS,
ffi.PRDT_SFTY_RSPNS_DS,
ffi.DRVR_ARBG_STS_NM,
ffi.VHCL_DRVR_INJRY_IN,
ffi.PWRTRN_OBSVTN_TX,
ffi.EXTR_BDY_INCDNT_TX,
ffi.INCDNT_IVSTGN_RPT_FORM_ID,
ffi.RMVD_ALGD_DFCTV_PRT_TX,
ffi.INTRR_INFRMN_TX,
ffi.LFT_REAR_ARBG_STS_NM,
ffi.LFT_REAR_VHCL_OCPNT_INJRY_IN,
ffi.VHCL_PRKD_DRTN_TX,
ffi.INCDNT_LCTN_TX,
ffi.MN_ALGTN_NM,
ffi.ISPCTN_MLG_NB,
ffi.PRDT_SFTY_MNTR_IN,
ffi.PRPRTY_DMG_DS,
ffi.OTHR_VHCL_DMG_DS,
ffi.PRSN_INVLVD_NB,
ffi.OTHR_SPLMNT_RPT_NM,
ffi.ADTNL_VHCL_INFRMN_TX,
ffi.OTHR_VHCL_OCPNT_INJRY_IN,
ffi.OTHR_ARBG_STS_NM,
ffi.OTHR_VHCL_TYP_TX,
ffi.VHCL_CRNT_LCTN_TX,
ffi.INCDNT_SHRT_DS,
ffi.PRDT_SFTY_RSPNS_TX,
ffi.RSTRNT_SYSTM_SPLMNT_RPT_IN,
ffi.RGHT_FRNT_VHCL_OCPNT_INJRY_DS,
ffi.RGHT_FRNT_ARBG_STS_NM,
ffi.RGHT_FRNT_VHCL_OCPNT_INJRY_IN,
ffi.RGHT_REAR_ARBG_STS_NM,
ffi.RGHT_REAR_VHCL_OCPNT_INJRY_IN,
ffi.SEAT_RSTRNT_OBSVTN_TX,
ffi.SCNDRY_ALGTN_NM,
ffi.STERNG_SPLMNT_RPT_IN,
ffi.THRML_SPLMNT_RPT_IN,
ffi.TRNMSN_SPLMNT_RPT_IN,
ffi.UINTD_ACLRT_SPLMNT_RPT_IN,
ffi.UNDR_CARG_OBSVTN_TX,
ffi.ESTMTD_VHCL_SPD_TX,
ffi.IVSTGN_VIN_ID,
ffi.WTHR_CNDTN_TX,
ffi.INCDNT_IVSTGN_RPT_PDF_URL_TX,
ffi.INCDNT_DTL_DS,
ffi.CSTMR_RQST_TX,
ffi.INCDNT_VHCL_MDL_YR_NB,
ffi.INCDNT_VHCL_NMC_MDL_CD,
ffi.INCDNT_VHCL_MK_NM,
ffi.ATCHMT_IN,
dv.VHCL_NMC_MDL_NM,
dv.DRV_TRN_DS,
dv.DRV_TRN_CD,
dv.TRM_LVL_DS,
dv.MNFCR_DT,
dv.NML_PRDCTN_MDL_CD,
dv.ORGNL_IN_SVC_DT,
dv.TRNMSN_TYP_CD,
dv.TRNMSN_TYP_NM,
dv.EMSN_CRFCTN_NB,
ffi.CRTE_TS AS UPDT_TS
from
(select iv.INCDNT_DM_KY, iv.INCDNT_TYP_DM_KY, max(iv.trd_dm_ky) as max_trd_dm_ky
from (select ffi.INCDNT_DM_KY,
ffi.INCDNT_TYP_DM_KY,
ffi.TRD_DM_KY,
ffi.TRNSCN_DT_KY,
ffi.TRD_EFCTV_DT_KY,
ffi.VHCL_ISPCTN_DT_KY,
ffi.VHCL_DM_KY,
ffi.VHCL_MDL_DM_KY,
ffi.CLMNT_ST_DM_KY,
ffi.CLMNT_CNTRY_DM_KY,
ffi.ESTMTD_SPD_TX,
ffi.OTHR_VHCL_INVLV_IN,
ffi.PRPRTY_DS,
ffi.ADTNL_OBSVTN_TX,
ffi.VHCL_ADVNCD_TCHLGY_IN,
ffi.ARBG_OBSVTN_TX,
ffi.ALGTN_CMPNT_LVL_1_NM,
ffi.ALGTN_CMPNT_LVL_2_NM,
ffi.ALGTN_CMPNT_LVL_3_NM,
ffi.BRAK_SPLMNT_IN,
ffi.SLSFRC_CASE_ID,
ffi.CTR_REAR_VHCL_OCPNT_INJRY_IN,
ffi.INCDNT_OCRNC_TS,
ffi.VHCL_DRVR_INJRY_DS,
ffi.CTR_REAR_VHCL_OCPNT_INJRY_DS,
ffi.LFT_REAR_VHCL_OCPNT_INJRY_DS,
ffi.OTHR_VHCL_OCPNT_INJRY_DS,
ffi.RGHT_REAR_VHCL_OCPNT_INJRY_DS,
ffi.ALGTN_DTL_DS,
ffi.PRDT_SFTY_RSPNS_DS,
ffi.DRVR_ARBG_STS_NM,
ffi.VHCL_DRVR_INJRY_IN,
ffi.PWRTRN_OBSVTN_TX,
ffi.EXTR_BDY_INCDNT_TX,
ffi.INCDNT_IVSTGN_RPT_FORM_ID,
ffi.RMVD_ALGD_DFCTV_PRT_TX,
ffi.INTRR_INFRMN_TX,
ffi.LFT_REAR_ARBG_STS_NM,
ffi.LFT_REAR_VHCL_OCPNT_INJRY_IN,
ffi.VHCL_PRKD_DRTN_TX,
ffi.INCDNT_LCTN_TX,
ffi.MN_ALGTN_NM,
ffi.ISPCTN_MLG_NB,
ffi.PRDT_SFTY_MNTR_IN,
ffi.INCDNT_IVSTGN_RPT_FORM_NM,
ffi.PRPRTY_DMG_DS,
ffi.OTHR_VHCL_DMG_DS,
ffi.PRSN_INVLVD_NB,
ffi.OTHR_SPLMNT_RPT_NM,
ffi.ADTNL_VHCL_INFRMN_TX,
ffi.OTHR_VHCL_OCPNT_INJRY_IN,
ffi.OTHR_ARBG_STS_NM,
ffi.OTHR_VHCL_TYP_TX,
ffi.VHCL_CRNT_LCTN_TX,
ffi.INCDNT_SHRT_DS,
ffi.PRDT_SFTY_RSPNS_TX,
ffi.RSTRNT_SYSTM_SPLMNT_RPT_IN,
ffi.RGHT_FRNT_VHCL_OCPNT_INJRY_DS,
ffi.RGHT_FRNT_ARBG_STS_NM,
ffi.RGHT_FRNT_VHCL_OCPNT_INJRY_IN,
ffi.RGHT_REAR_ARBG_STS_NM,
ffi.RGHT_REAR_VHCL_OCPNT_INJRY_IN,
ffi.SEAT_RSTRNT_OBSVTN_TX,
ffi.SCNDRY_ALGTN_NM,
ffi.STERNG_SPLMNT_RPT_IN,
ffi.THRML_SPLMNT_RPT_IN,
ffi.TRNMSN_SPLMNT_RPT_IN,
ffi.UINTD_ACLRT_SPLMNT_RPT_IN,
ffi.UNDR_CARG_OBSVTN_TX,
ffi.ESTMTD_VHCL_SPD_TX,
ffi.IVSTGN_VIN_ID,
ffi.WTHR_CNDTN_TX,
ffi.INCDNT_IVSTGN_RPT_PDF_URL_TX,
ffi.INCDNT_DTL_DS,
ffi.CSTMR_RQST_TX,
ffi.INCDNT_VHCL_MDL_YR_NB,
ffi.INCDNT_VHCL_NMC_MDL_CD,
ffi.INCDNT_VHCL_MK_NM,
ffi.ATCHMT_IN,
ffi.CNSMR_AFR_INSTNC_CN,
ffi.CRTE_TS,
ffi.CRTE_USR_ID
from
(select c.iNCDNT_DM_KY, c.INCDNT_TYP_DM_KY, max(c.TRNSCN_DT_KY) as iir_tns_dt
from (
    SELECT D.INCDNT_DM_KY, D.INCDNT_TYP_DM_KY, D.TRNSCN_DT_KY
    FROM BID_LGL_IIR.INCDNT_IVSTGN_FC D
    ORDER BY D.TRNSCN_DT_KY DESC ) c
group by c.iNCDNT_DM_KY, c.INCDNT_TYP_DM_KY) list,
BID_LGL_IIR.INCDNT_IVSTGN_FC ffi
where list.INCDNT_DM_KY = ffi.INCDNT_DM_KY
and list.INCDNT_TYP_DM_KY = ffi.INCDNT_TYP_DM_KY
and list.iir_TNS_DT = ffi.TRNSCN_DT_KY) iv
group by iv.INCDNT_DM_KY, iv.INCDNT_TYP_DM_KY) a
join (select ffi.INCDNT_DM_KY,
ffi.INCDNT_TYP_DM_KY,
ffi.TRD_DM_KY,
ffi.TRNSCN_DT_KY,
ffi.TRD_EFCTV_DT_KY,
ffi.VHCL_ISPCTN_DT_KY,
ffi.VHCL_DM_KY,
ffi.VHCL_MDL_DM_KY,
ffi.CLMNT_ST_DM_KY,
ffi.CLMNT_CNTRY_DM_KY,
ffi.ESTMTD_SPD_TX,
ffi.OTHR_VHCL_INVLV_IN,
ffi.PRPRTY_DS,
ffi.ADTNL_OBSVTN_TX,
ffi.VHCL_ADVNCD_TCHLGY_IN,
ffi.ARBG_OBSVTN_TX,
ffi.ALGTN_CMPNT_LVL_1_NM,
ffi.ALGTN_CMPNT_LVL_2_NM,
ffi.ALGTN_CMPNT_LVL_3_NM,
ffi.BRAK_SPLMNT_IN,
ffi.SLSFRC_CASE_ID,
ffi.CTR_REAR_VHCL_OCPNT_INJRY_IN,
ffi.INCDNT_OCRNC_TS,
ffi.VHCL_DRVR_INJRY_DS,
ffi.CTR_REAR_VHCL_OCPNT_INJRY_DS,
ffi.LFT_REAR_VHCL_OCPNT_INJRY_DS,
ffi.OTHR_VHCL_OCPNT_INJRY_DS,
ffi.RGHT_REAR_VHCL_OCPNT_INJRY_DS,
ffi.ALGTN_DTL_DS,
ffi.PRDT_SFTY_RSPNS_DS,
ffi.DRVR_ARBG_STS_NM,
ffi.VHCL_DRVR_INJRY_IN,
ffi.PWRTRN_OBSVTN_TX,
ffi.EXTR_BDY_INCDNT_TX,
ffi.INCDNT_IVSTGN_RPT_FORM_ID,
ffi.RMVD_ALGD_DFCTV_PRT_TX,
ffi.INTRR_INFRMN_TX,
ffi.LFT_REAR_ARBG_STS_NM,
ffi.LFT_REAR_VHCL_OCPNT_INJRY_IN,
ffi.VHCL_PRKD_DRTN_TX,
ffi.INCDNT_LCTN_TX,
ffi.MN_ALGTN_NM,
ffi.ISPCTN_MLG_NB,
ffi.PRDT_SFTY_MNTR_IN,
ffi.INCDNT_IVSTGN_RPT_FORM_NM,
ffi.PRPRTY_DMG_DS,
ffi.OTHR_VHCL_DMG_DS,
ffi.PRSN_INVLVD_NB,
ffi.OTHR_SPLMNT_RPT_NM,
ffi.ADTNL_VHCL_INFRMN_TX,
ffi.OTHR_VHCL_OCPNT_INJRY_IN,
ffi.OTHR_ARBG_STS_NM,
ffi.OTHR_VHCL_TYP_TX,
ffi.VHCL_CRNT_LCTN_TX,
ffi.INCDNT_SHRT_DS,
ffi.PRDT_SFTY_RSPNS_TX,
ffi.RSTRNT_SYSTM_SPLMNT_RPT_IN,
ffi.RGHT_FRNT_VHCL_OCPNT_INJRY_DS,
ffi.RGHT_FRNT_ARBG_STS_NM,
ffi.RGHT_FRNT_VHCL_OCPNT_INJRY_IN,
ffi.RGHT_REAR_ARBG_STS_NM,
ffi.RGHT_REAR_VHCL_OCPNT_INJRY_IN,
ffi.SEAT_RSTRNT_OBSVTN_TX,
ffi.SCNDRY_ALGTN_NM,
ffi.STERNG_SPLMNT_RPT_IN,
ffi.THRML_SPLMNT_RPT_IN,
ffi.TRNMSN_SPLMNT_RPT_IN,
ffi.UINTD_ACLRT_SPLMNT_RPT_IN,
ffi.UNDR_CARG_OBSVTN_TX,
ffi.ESTMTD_VHCL_SPD_TX,
ffi.IVSTGN_VIN_ID,
ffi.WTHR_CNDTN_TX,
ffi.INCDNT_IVSTGN_RPT_PDF_URL_TX,
ffi.INCDNT_DTL_DS,
ffi.CSTMR_RQST_TX,
ffi.INCDNT_VHCL_MDL_YR_NB,
ffi.INCDNT_VHCL_NMC_MDL_CD,
ffi.INCDNT_VHCL_MK_NM,
ffi.ATCHMT_IN,
ffi.CNSMR_AFR_INSTNC_CN,
ffi.CRTE_TS,
ffi.CRTE_USR_ID
from
(select c.iNCDNT_DM_KY, c.INCDNT_TYP_DM_KY, max(c.TRNSCN_DT_KY) as iir_tns_dt
from (
    SELECT D.INCDNT_DM_KY, D.INCDNT_TYP_DM_KY, D.TRNSCN_DT_KY
    FROM BID_LGL_IIR.INCDNT_IVSTGN_FC D
    ORDER BY D.TRNSCN_DT_KY DESC ) c
group by c.iNCDNT_DM_KY, c.INCDNT_TYP_DM_KY) list,
BID_LGL_IIR.INCDNT_IVSTGN_FC ffi
where list.INCDNT_DM_KY = ffi.INCDNT_DM_KY
and list.INCDNT_TYP_DM_KY = ffi.INCDNT_TYP_DM_KY
and list.iir_TNS_DT = ffi.TRNSCN_DT_KY) ffi
on a.INCDNT_DM_KY = ffi.INCDNT_DM_KY
and a.INCDNT_TYP_DM_KY = ffi.INCDNT_TYP_DM_KY
and a.max_trd_dm_ky = ffi.trd_dm_ky
left outer join bid_veh.vhcl_dm dv on dv.VHCL_DM_KY = ffi.VHCL_DM_KY and dv.VHCL_DM_END_DT > sysdate
left outer join BID_VEH.ST_DM ds on ds.ST_DM_KY = ffi.CLMNT_ST_DM_KY
left outer join BID_VEH.CNTRY_DM DC on DC.CNTRY_DM_KY = ffi.CLMNT_CNTRY_DM_KY
left outer join NEDW_DIM.CLNDR_DM DET on DET.clndr_dm_ky = ffi.TRD_EFCTV_DT_KY
left outer join NEDW_DIM.CLNDR_DM dt on dt.clndr_dm_ky = ffi.TRNSCN_DT_KY
left outer join NEDW_DIM.CLNDR_DM DIV on DIV.clndr_dm_ky = ffi.VHCL_ISPCTN_DT_KY)
 WHERE \$CONDITIONS" --target-dir hdfs://bdestg/data/lgl_iir/raw/incdnt_ivstgn_raw --fields-terminated-by '\001' --null-string '' --null-non-string '' --escaped-by '\\' --check-column UPDT_TS --map-column-java INCDNT_DTL_DS=String --incremental 'lastmodified' --hive-drop-import-delims --merge-key INCDNT_IVSTGN_RPT_FORM_NM --last-value '0001-01-01 00:00:00.000' --username 'BIDBATCH' --password 'nissan' -m 1