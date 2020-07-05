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

# Claims
# CA
# IR
# AVES
# Techline
# Inspect
# Vehicle
# PFP
# MQR
# EQUIP Ingest & Monitoring
# QCS
# Gears

# ResumeEquip Master Deployment Log		https://confluence.na.nissan.biz/pages/viewpage.action?spaceKey=TCSFQI&title=EQUIP+Master+Deployment+Log
# https://text-compare.com/


set PATH="C:\Program Files (x86)\Java\jdk1.8.0_152\bin";%PATH%;"D:\Sivakumar\TechM-Nissan\Daily Health Checkup\Zeppelin_Notebooks\apache-maven-3.6.3\bin"

# mvn install:install-file -Dfile="D:\Sivakumar\TechM-Nissan\Daily Health Checkup\Zeppelin_Notebooks\apache-maven-3.6.3\lib\commons-io-2.5.jar" -DpomFile="D:\Sivakumar\TechM-Nissan\Daily Health Checkup\Zeppelin_Notebooks\Json2Flat-master\json2flat\pom.xml"

CNSMR_AFR_BASE
CNSMR_AFR_CMNT_BASE
CNSMR_AFR_TRD_CNCRN_BASE
/projects/equip/scripts/spark/ca_ingest.py
/projects/common/*
/projects/common/spark2_submit.sh

prts_dtl_mrkt_rply_pblc


# *****************************************************************************************

# EQUIP properties files location
http://svn.na.nissan.biz/svn/NNA/NNA_TCS_FQI_RCode/branches/EQUIP/equip_reporting/Prd/properties/claims.properties
http://svn.na.nissan.biz/svn/NNA/NNA_TCS_FQI_RCode/branches/EQUIP/equip_reporting/Prd/properties/consumeraffairs.properties
http://svn.na.nissan.biz/svn/NNA/NNA_TCS_FQI_RCode/branches/EQUIP/equip_reporting/Prd/properties/equip.properties
http://svn.na.nissan.biz/svn/NNA/NNA_TCS_FQI_RCode/branches/EQUIP/equip_reporting/Prd/properties/gear2.properties
http://svn.na.nissan.biz/svn/NNA/NNA_TCS_FQI_RCode/branches/EQUIP/equip_reporting/Prd/properties/gears2.0-rawTables.properties
http://svn.na.nissan.biz/svn/NNA/NNA_TCS_FQI_RCode/branches/EQUIP/equip_reporting/Prd/properties/gears3-0.properties
http://svn.na.nissan.biz/svn/NNA/NNA_TCS_FQI_RCode/branches/EQUIP/equip_reporting/Prd/properties/incident_rate.properties
http://svn.na.nissan.biz/svn/NNA/NNA_TCS_FQI_RCode/branches/EQUIP/equip_reporting/Prd/properties/monitor.properties
http://svn.na.nissan.biz/svn/NNA/NNA_TCS_FQI_RCode/branches/EQUIP/equip_reporting/Prd/properties/qcs.properties
http://svn.na.nissan.biz/svn/NNA/NNA_TCS_FQI_RCode/branches/EQUIP/equip_reporting/Prd/properties/solr_monitor.properties
http://svn.na.nissan.biz/svn/NNA/NNA_TCS_FQI_RCode/branches/EQUIP/equip_reporting/Prd/properties/techline.properties

# EQUIP Worklow/coordinator location
/projects/equip/oozie/Claims-Condense-Weekly/*
/projects/equip/oozie/Claims-Ingest-Daily/*
/projects/equip/oozie/ConsumerAffairs-Condense-Weekly/*
/projects/equip/oozie/ConsumerAffairs-Ingest-Daily/*
/projects/equip/oozie/EQUIP-AVES-Condense-Weekly/*
/projects/equip/oozie/EQUIP-AVES-Ingest-Daily/*
/projects/equip/oozie/EQUIP-Claims-Condense-Daily/*
/projects/equip/oozie/EQUIP-Claims-Condense-Weekly/*
/projects/equip/oozie/EQUIP-Claims-Solr-Daily/*
/projects/equip/oozie/EQUIP-ConsumerAffairs-Condense-Weekly/*
/projects/equip/oozie/EQUIP-ConsumerAffairs-Solr-Daily/*
/projects/equip/oozie/EQUIP-GEARS2.0-create-raw-tables-daily/*
/projects/equip/oozie/EQUIP-GEARS3.0-adoc/*
/projects/equip/oozie/EQUIP-Gear2-Monthly/*
/projects/equip/oozie/EQUIP-INSPECT-Condense-Weekly/*
/projects/equip/oozie/EQUIP-INSPECT-Ingest-Daily/*
/projects/equip/oozie/EQUIP-Monitor-Daily/*
/projects/equip/oozie/EQUIP-SOLR-Monitor-Daily/*
/projects/equip/oozie/EQUIP-Techline-Condense-Weekly/*
/projects/equip/oozie/EQUIP-Techline-Solr-Daily/*
/projects/equip/oozie/Equip-Warranty-Claims-Ingestion-Daily/*
/projects/equip/oozie/QCS-Survey-Daily/*
/projects/equip/oozie/QCS-Survey-Ingest-Fullload-Daily/*
/projects/equip/oozie/Techline-Condense-Weekly/*
/projects/equip/oozie/Techline-Ingest-Daily/*
/projects/equip/oozie/claims.xml
/projects/equip/oozie/claims_extracts/*
/projects/equip/oozie/claims_onetime/*
/projects/equip/oozie/claims_solr_incremental/*
/projects/equip/oozie/consumeraffairs.xml
/projects/equip/oozie/equip.xml
/projects/equip/oozie/incident_rate_daily/*
/projects/equip/oozie/ingestion/*
/projects/equip/oozie/pfp/*
/projects/equip/oozie/qcs.xml
/projects/equip/oozie/techline.xml
/projects/equip/oozie/vehicle_onetime/*
/projects/equip/oozie/vehicle_solr_incremental/*


# EWS properties file loacation
http://svn.na.nissan.biz/svn/NNA/NNA_TCS_EWS_BDE/trunk/prd/oozie/ews.properties
http://svn.na.nissan.biz/svn/NNA/NNA_TCS_EWS_BDE/trunk/prd/oozie/ro-cleansing.properties



'name=EQUIP-AVES-Condense-Weekly-BDE-STG-wf;name=EQUIP-AVES-Ingest-Daily-BDE-PRD-wf;name=ConsumerAffairs-Condense-Weekly-BDE-PRD-wf'



DIH	PRD	http://10.78.11.31:18080/dih-console/login.jsp
DIH Non	PRD	http://10.78.11.32:18080/dih-console/login.jsp
Oozie	PRD	http://usnencpl075.nmcorp.nissan.biz:11000/oozie/
Oozie	STG	http://usnencpl077.nmcorp.nissan.biz:11000/oozie/
Oozie	DEV	http://usnencpl079.nmcorp.nissan.biz:11000/oozie/
Ambari	PRD	http://usnencpl075.nmcorp.nissan.biz:8080/#/login
Ambari	STG	http://usnencpl077.nmcorp.nissan.biz:8080/#/login
Ambari	DEV	http://usnencpl079.nmcorp.nissan.biz:8080/#/login
Zeppelin	PRD	http://usnencpl075.nmcorp.nissan.biz:9995/
Zeppelin	STG	http://usnencpl077.nmcorp.nissan.biz:9995/
Zeppelin	DEV	http://usnencpl079.nmcorp.nissan.biz:9995/
Tableau	PRD	https://tableau.na.nissan.biz/#/site/TCS/datasources
Solr	STG	http://usnencpl091.nmcorp.nissan.biz:8983/solr/#/
EQUIP	PRD	https://datamgtanalytics.na.nissan.biz/
EQUIP	STG	https://datamgtanalytics.stage.na.nissan.biz/
Confluence		https://confluence.na.nissan.biz/
Jira		https://jira.na.nissan.biz/
Zoom		https://nissan-usa.zoom.us/j/8647358496
ServiceNow		https://nnanissan.service-now.com/





Claims-Ingest-Daily-BDE-PRD-wf, Skipped thrice, Now RUNNING
ConsumerAffairs-Ingest-Daily-BDE-PRD-wf, Skipped thrice, Now SUCCEEDED
DBSRO-data-alert-BDE-PRD-wf, Now SKIPPED for today
dbsro-wf, SUUCEEDED up to 13:25 GMT, Now WAITING
EQUIP-AVES-Ingest-Daily-BDE-PRD-wf,
EQUIP-ConsumerAffairs-Solr-Daily-BDE-PRD-wf, Skipped thrice, Now SUCCEEDED
EQUIP-incident-rate-daily-wf, Skipped once, Now SUCCEEDED
EQUIP-INSPECT-Ingest-Daily-BDE-PRD-wf, Skipped twice, Now SUCCEEDED
EQUIP-Monitor-Ingestion-Daily-BDE-PRD-wf, Skipped Now
EQUIP-MQR-Source-Ingestion-Daily-wf, Skipped Now
EQUIP-pfp-daily-ingest-wf, Skipped once, Now SUCCEEDED
EQUIP-Techline-Solr-Daily-BDE-PRD-wf, Skipped thrice, Now running
EWS-Drive-Common-Ingest-Daily-Prd-wf, Some hourly job killed, Now SUCCEEDED
EWS-Drive-Ingest-Inc-daily-Prd-wf,
EWS-DRIVE-tr-Solr-Indexing-Daily-Prd-wf,
EWS-ML-Predict-Daily-Prd-wf,
shell-wf, KILLED twice, Now SUCCEEDED
Techline-Ingest-Daily-BDE-PRD-wf, Skipped twice, Now SECCEEDED



EWS-CA-Solr-Indexing-Daily-Prd-wf
EWS-FIR-Ingest-Incremental-Daily-Prd-wf
EWS-NHTSA-IVSTGN-Ingest-Daily-Prd-wf
EWS-NHTSA-RCL-Ingest-Daily-Prd-wf
EWS-NHTSA-SVC-Ingest-Daily-Prd-wf
EWS-PRJ-Ingest-Incremental-Daily-Prd-wf
EWS-RCL-Ingest-Daily-Prd-wf
EWS-RO-Cleansing-Daily-Prd-wf
EWS-RO-Solr-Indexing-Daily-Prd-wf
