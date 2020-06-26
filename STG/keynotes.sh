sqoop import --connect jdbc:oracle:thin:@10.78.78.150:58532/ODDBEFLO --query "select INCDNT_IVSTGN_RPT_FORM_NM,TRD_CD,TRD_CD_DS,CAST('x987731' as VARCHAR(10)) as CRTE_USR_ID,CURRENT_TIMESTAMP as CRTE_TS,UPDT_TS,CMPST_KY from BID_TRD_VS.INCDNT_IVSTGN_TRD_EXTRCT_VW WHERE \$CONDITIONS" --target-dir hdfs://bdedev/data/lgl_iir/raw/incdnt_ivstgn_trd_raw_test --fields-terminated-by '\001' --null-string '' --null-non-string '' --escaped-by '\\' --check-column UPDT_TS --incremental 'lastmodified' --hive-drop-import-delims --merge-key INCDNT_IVSTGN_RPT_FORM_NM --last-value '0001-01-01 00:00:00.000' --username BDESELECT --password-file hdfs://bdedev/projects/ews/lib/.bidwPassword -m 1

sqoop import --connect jdbc:oracle:thin:@10.78.78.150:58532/ODDBEFLO --query "select INCDNT_IVSTGN_RPT_FORM_NM,TRD_CD,TRD_CD_DS,CAST('x987731' as VARCHAR(10)) as CRTE_USR_ID,CURRENT_TIMESTAMP as CRTE_TS,UPDT_TS,CMPST_KY from BID_TRD_VS.INCDNT_IVSTGN_TRD_EXTRCT_VW WHERE \$CONDITIONS" --target-dir hdfs://bdedev/data/lgl_iir/raw/incdnt_ivstgn_trd_raw_test --fields-terminated-by '\001' --null-string '' --null-non-string '' --escaped-by '\\' --check-column UPDT_TS --incremental 'lastmodified' --hive-drop-import-delims --merge-key INCDNT_IVSTGN_RPT_FORM_NM --last-value 'lastvalue' --username BDESELECT --password-file hdfs://bdedev/projects/ews/lib/.bidwPassword -m 1

sqoop import --connect jdbc:oracle:thin:@10.78.78.155:58532/OSDBEFLO --query "select INCDNT_IVSTGN_RPT_FORM_NM,TRNSCN_DT,ESTMTD_SPD_TX,OTHR_VHCL_INVLV_IN,PRPRTY_DS,TRD_EFCTV_DT,VHCL_ISPCTN_DT,VHCL_ID_NB,VHCL_MK_NM,VHCL_YR_NB,VHCL_NMC_MDL_CD,CLMNT_ST_CD,CLMNT_ST_NM,CLMNT_CNTRY_CD,ADTNL_OBSVTN_TX,VHCL_ADVNCD_TCHLGY_IN,ARBG_OBSVTN_TX,ALGTN_CMPNT_LVL_1_NM,ALGTN_CMPNT_LVL_2_NM,ALGTN_CMPNT_LVL_3_NM,BRAK_SPLMNT_IN,SLSFRC_CASE_ID,CTR_REAR_VHCL_OCPNT_INJRY_IN,INCDNT_OCRNC_TS,VHCL_DRVR_INJRY_DS,CTR_REAR_VHCL_OCPNT_INJRY_DS,LFT_REAR_VHCL_OCPNT_INJRY_DS,OTHR_VHCL_OCPNT_INJRY_DS,RGHT_REAR_VHCL_OCPNT_INJRY_DS,ALGTN_DTL_DS,PRDT_SFTY_RSPNS_DS,DRVR_ARBG_STS_NM,VHCL_DRVR_INJRY_IN,PWRTRN_OBSVTN_TX,EXTR_BDY_INCDNT_TX,INCDNT_IVSTGN_RPT_FORM_ID,RMVD_ALGD_DFCTV_PRT_TX,INTRR_INFRMN_TX,LFT_REAR_ARBG_STS_NM,LFT_REAR_VHCL_OCPNT_INJRY_IN,VHCL_PRKD_DRTN_TX,INCDNT_LCTN_TX,MN_ALGTN_NM,ISPCTN_MLG_NB,PRDT_SFTY_MNTR_IN,PRPRTY_DMG_DS,OTHR_VHCL_DMG_DS,PRSN_INVLVD_NB,OTHR_SPLMNT_RPT_NM,ADTNL_VHCL_INFRMN_TX,OTHR_VHCL_OCPNT_INJRY_IN,OTHR_ARBG_STS_NM,OTHR_VHCL_TYP_TX,VHCL_CRNT_LCTN_TX,INCDNT_SHRT_DS,PRDT_SFTY_RSPNS_TX,RSTRNT_SYSTM_SPLMNT_RPT_IN,RGHT_FRNT_VHCL_OCPNT_INJRY_DS,RGHT_FRNT_ARBG_STS_NM,RGHT_FRNT_VHCL_OCPNT_INJRY_IN,RGHT_REAR_ARBG_STS_NM,RGHT_REAR_VHCL_OCPNT_INJRY_IN,SEAT_RSTRNT_OBSVTN_TX,SCNDRY_ALGTN_NM,STERNG_SPLMNT_RPT_IN,THRML_SPLMNT_RPT_IN,TRNMSN_SPLMNT_RPT_IN,UINTD_ACLRT_SPLMNT_RPT_IN,UNDR_CARG_OBSVTN_TX,ESTMTD_VHCL_SPD_TX,IVSTGN_VIN_ID,WTHR_CNDTN_TX,INCDNT_IVSTGN_RPT_PDF_URL_TX,INCDNT_DTL_DS,CSTMR_RQST_TX,INCDNT_VHCL_MDL_YR_NB,INCDNT_VHCL_NMC_MDL_CD,INCDNT_VHCL_MK_NM,ATCHMT_IN,VHCL_NMC_MDL_NM,DRV_TRN_DS,DRV_TRN_CD,TRM_LVL_DS,MNFCR_DT,NML_PRDCTN_MDL_CD,ORGNL_IN_SVC_DT,TRNMSN_TYP_CD,TRNMSN_TYP_NM, EMSN_CRFCTN_NB,CURRENT_TIMESTAMP as CRTE_TS,CAST(' x987731' as VARCHAR(10)) as CRTE_USR_ID,UPDT_TS from  BID_TRD_VS.INCDNT_IVSTGN_EXTRCT_VW WHERE \$CONDITIONS" --target-dir hdfs://bdestg/data/lgl_iir/raw/incdnt_ivstgn_raw --fields-terminated-by '\001' --null-string '' --null-non-string '' --escaped-by '\\' --check-column UPDT_TS --map-column-java INCDNT_DTL_DS=String --incremental 'lastmodified' --hive-drop-import-delims --merge-key INCDNT_IVSTGN_RPT_FORM_NM --last-value 'lastvalue' --username 'BDESELECT' --password-file hdfs://bdestg/projects/ews/lib/.bidwPassword -m 1


sqoop import --connect jdbc:oracle:thin:@10.78.78.155:58532/OSDBEFLO --query "select INCDNT_IVSTGN_RPT_FORM_NM,TRD_CD,TRD_CD_DS,CAST('x987731' as VARCHAR(10)) as CRTE_USR_ID,CURRENT_TIMESTAMP as CRTE_TS,CMPST_KY,UPDT_TS from BID_TRD_VS.INCDNT_IVSTGN_TRD_EXTRCT_VW WHERE \$CONDITIONS" --target-dir hdfs://bdestg/data/lgl_iir/raw/incdnt_ivstgn_trd_raw --fields-terminated-by '\001' --null-string '' --null-non-string '' --escaped-by '\\' --check-column UPDT_TS --incremental 'lastmodified' --hive-drop-import-delims --merge-key INCDNT_IVSTGN_RPT_FORM_NM --last-value '0001-01-01 00:00:00.000' --username 'BDESELECT' --password-file hdfs://bdestg/projects/ews/lib/.bidwPassword -m 1



sqoop import --connect jdbc:oracle:thin:@usnencvl916.nmcorp.nissan.biz:58532/ODDBFQIA --username TCS_ETL --password 'Nissan123$' --query "SELECT CSTMR_STSFCN_TEAM_KY,CSTMR_STSFCN_PARNT_TEAM_KY,CSTMR_STSFCN_TEAM_CD,CSTMR_STSFCN_TEAM_NM FROM MNS_TCS_TRN.CSTMR_STSFCN_TEAM WHERE \$CONDITIONS" --target-dir hdfs://bdedev/data/equip/cstmr_stsfcn_team --fields-terminated-by '|' -m 1

sqoop import --connect jdbc:oracle:thin:@usnencvl728.nmcorp.nissan.biz:58532/OSDBFQIA --username TCS_ETL --password 'nissan21' --query "SELECT CSTMR_STSFCN_TEAM_KY,CSTMR_STSFCN_PARNT_TEAM_KY,CSTMR_STSFCN_TEAM_CD,CSTMR_STSFCN_TEAM_NM FROM MNS_TCS_TRN.CSTMR_STSFCN_TEAM WHERE \$CONDITIONS" --target-dir hdfs://bdedev/data/equip/cstmr_stsfcn_team --fields-terminated-by '|' -m 1

select c.CSTMR_STSFCN_TEAM_KY, c.CSTMR_STSFCN_TEAM_CD, c.CSTMR_STSFCN_TEAM_NM from MNS_TCS_TRN.CSTMR_STSFCN_TEAM c
INNER JOIN MNS_TCS_TRN.FQI_PFP f on c.CSTMR_STSFCN_TEAM_KY=f.CSTMR_STSFCN_TEAM_KY

select rcvd_pfp_cmpnt_cstmr_stsfcn_team_ds, updt_ts from wrnty_clm_vhcl_solr where DATE(updt_ts)="2020-06-19"

/projects/ews/scripts/spark/search/dependencies.zip
/projects/ews/oozie/drive-tr-ingest/drive_tr_workflow.xml

EQUIP
kinit -kt /etc/security/keytabs/X981138.keytab x981138@NMCORP.NISSAN.BIZ
EWS:
kinit -kt /etc/security/keytabs/x987731.keytab x987731@NMCORP.NISSAN.BIZ

hdfs dfs -cp /projects/ews/scripts/spark/search/conf/drive_sqoop/sqoop_*.xml /projects/ews/scripts/spark/search/conf/iir_sqoop/
oozie job --oozie http://usnencpl077.nmcorp.nissan.biz:11000/oozie --config sqoop_arch.properties -run


hdfs dfs -rm /data/lgl_iir/raw/incdnt_ivstgn_raw/*
hdfs dfs -rm /data/lgl_iir/raw/incdnt_ivstgn_raw/

incdnt_ivstgn_raw
incdnt_ivstgn_trd_raw
incdnt_ivstgn_base
incdnt_ivstgn_trd_base


EWS-Drive-Common-Ingest-Daily-Stg-wf
EWS-IIR-Ingest-Inc-daily-Stg-wf
EWS-drive-Ingest-Inc-daily-Stg-wf

Entry delete - IIR with N Indicator


INSERT OVERWRITE table bde_stats.DATA_EXTRCN_RULE VALUES('EWS-Drive-Ingest-Inc-daily','gcars',1,'sqoop-dflt',8,'EWS_Drive_Ingest_Inc_daily.ini','/projects/ews/scripts/spark/search/conf','/etc/security/keytabs/x987731.keytab','x987731@NMCORP.NISSAN.BIZ','N','drive_sqoop','2019-12-04 00:00:00','drive_sqoop','2019-12-04 00:00:00'), ('EWS-IIR-Ingest-Inc-daily','iir',1,'sqoop-dflt',9,'EWS_IIR_Ingest_Inc_daily.ini','/projects/ews/scripts/spark/search/conf','','/etc/security/keytabs/x987731.keytab x987731@NMCORP.NISSAN.BIZ','N','drive_sqoop','2020-06-01','drive_sqoop','2020-06-01');


Claims
CA
IR
AVES
Techline
Inspect
Vehicle
PFP
MQR
EQUIP Ingest & Monitoring
QCS
Gears
