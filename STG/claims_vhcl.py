import sys, time
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

def getEnrichedData(spark, filterQuery = None):
	warrantyClaims = spark.read.table(database + ".wrnty_clm_crnt_vw").alias("wrnty_clm")
	vehicle = spark.read.table(database + ".vhcl_solr")
	vehicleFiltered = vehicle.filter(vehicle['vhcl_frgmt_in'] != lit('Y')).filter(vehicle['vhcl_yr_nb'] > year(current_date()) - lit(5)).alias("vhcl")
	pfp = spark.read.table(database + ".pfp_crnt_vw").alias("pfp_dm")
	vendor1 = spark.read.table(database + ".vndr_crnt_vw").alias("fncl_splr")
	vendor2 = spark.read.table(database + ".vndr_crnt_vw").alias("rcvd_splr")
	pnc = spark.read.table(database + ".pnc_crnt_vw").alias("pnc_dm")
	fqiPFP = spark.read.table(database + ".fqi_pfp").alias("fqi_pfp")
	fqiCurrentModel = spark.read.table(database + ".FQI_CRNT_MDL").alias("fqi_crnt")
	pfpResponsibleEngineer = spark.read.table(database + ".PFP_RSPSBL_ENGNR").alias("rspsbl_engnr_nm")
	fqiRepresentatives = spark.read.table(database + ".FQI_RPSNTV").alias("fqi_rpsntv")
	fqiGroup = spark.read.table(database + ".FQI_GRP").alias("FQI_GRP")
	dealer = spark.read.table(database + ".DLR_DM")
	filteredDealer = dealer.filter(dealer['AFLT_CMPNY_CD'] == lit('NNA')).alias("svc_dlr")
	customerSatisfactionTeam = spark.read.table(database + ".cstmr_stsfcn_team").alias("team")
	assignedEngineer = spark.read.table(database + ".asgnd_engnr_crnt_vw").alias("asgnd_engnr")
	customerSatisfactionParentTeam = spark.read.table(database + ".cstmr_stsfcn_team").alias("parent_team")
	vehicleSymptoms = spark.read.table(database + ".vhcl_symptm_crnt_vw").alias("vhcl_symptm")
	vehicleTroubles = spark.read.table(database + ".vhcl_trbl_crnt_vw").alias("vhcl_trbl")
	distributors = spark.read.table(database + ".dstrbr_crnt_vw").alias("dstrbr")
	coverageCodes = spark.read.table(database + ".wrnty_cvrg_clas_crnt_vw").alias("cvrg")
	comments = spark.read.table(database + ".wrnty_cmnt_crnt_vw")
	customerComments = comments.filter(comments['wrnty_cmnt_typ_cd'] == lit('SVCAD')).withColumnRenamed('wrnty_clm_nb', 'cust_cmnt_wrnty_clm_nb').alias('cust_cmnt')
	technicianComments = comments.filter(comments['wrnty_cmnt_typ_cd'] == lit('TECH')).withColumnRenamed('wrnty_clm_nb', 'tech_cmnt_wrnty_clm_nb').alias('tech_cmnt')
	
	enrichedData = warrantyClaims.join(vehicleFiltered, warrantyClaims['dervd_vin_id'] == vehicleFiltered['vin_id'], 'inner') \
	        .join(broadcast(pfp), warrantyClaims['rcvd_pfp_1st_5_nb'] == pfp['pfp_nb'], 'left_outer') \
	        .join(broadcast(vendor1), warrantyClaims['src_splr_id'] == vendor1['vndr_nb'], 'left_outer') \
	        .join(broadcast(vendor2), warrantyClaims['rcvd_pfp_splr_nb'] == vendor2['vndr_nb'], 'left_outer') \
	        .join(broadcast(pnc), warrantyClaims['wrnty_orgnl_pnc_id'] == pnc['pnc_id'], 'left_outer') \
	        .join(broadcast(fqiPFP), warrantyClaims['FQI_WRNTY_PFP_NB'] == fqiPFP['fqi_pfp_nb'], 'left_outer') \
	        .join(broadcast(fqiCurrentModel), vehicle['nml_prdctn_mdl_cd'] == fqiCurrentModel['nml_prdctn_mdl_cd'], 'left_outer') \
	        .join(pfpResponsibleEngineer, [pfpResponsibleEngineer['nml_prdctn_base_mdl_cd'] == fqiCurrentModel['nml_prdctn_base_mdl_cd'], \
	                                                pfpResponsibleEngineer['fqi_pfp_nb'] == fqiPFP['fqi_pfp_nb'], \
	                                                vehicle['mnfctg_vhcl_plnt_cd'] == pfpResponsibleEngineer['vhcl_mnfctr_plnt_cd']], 'left_outer') \
	        .join(broadcast(fqiRepresentatives), pfpResponsibleEngineer['rpsntv_ky'] == fqiRepresentatives['rpsntv_ky'], 'left_outer') \
	        .join(broadcast(fqiGroup), fqiGroup['fqi_grp_ky'] == fqiPFP['fqi_grp_cd'], 'left_outer') \
	        .join(broadcast(filteredDealer), warrantyClaims['svc_dlr_nb'] == filteredDealer['DLR_NB'], 'left_outer') \
	        .join(broadcast(customerSatisfactionTeam), fqiPFP['cstmr_stsfcn_team_ky'] == customerSatisfactionTeam['cstmr_stsfcn_team_ky'], 'left_outer') \
	        .join(broadcast(assignedEngineer), [warrantyClaims['src_splr_id'] == assignedEngineer['src_splr_id'], \
	                                                warrantyClaims['plnt_asgnmt_cd'] == assignedEngineer['plnt_asgnmt_cd']], 'left_outer') \
	        .join(broadcast(customerSatisfactionParentTeam), customerSatisfactionParentTeam['cstmr_stsfcn_team_ky'] == customerSatisfactionTeam['cstmr_stsfcn_parnt_team_ky'], 'left_outer') \
	        .join(broadcast(vehicleSymptoms), vehicleSymptoms['symptm_cd'] == warrantyClaims['orgnl_symptm_cd'], 'left_outer') \
	        .join(broadcast(vehicleTroubles), vehicleTroubles['trbl_cd'] == warrantyClaims['orgnl_trbl_cd'], 'left_outer') \
	        .join(broadcast(distributors), distributors['dstrbr_cd'] == warrantyClaims['orgnl_dstrbr_cd'], 'left_outer') \
	        .join(broadcast(coverageCodes), coverageCodes['cvrg_clas_cd'] == warrantyClaims['cvrg_clas_cd'], 'left_outer') \
	        .join(customerComments, customerComments['cust_cmnt_wrnty_clm_nb'] == warrantyClaims['wrnty_clm_nb'], 'left_outer') \
	        .join(technicianComments, technicianComments['tech_cmnt_wrnty_clm_nb'] == warrantyClaims['wrnty_clm_nb'], 'left_outer')
	
	# First half of the final table wrnty_clm_vhcl_solr
	enrichedData = enrichedData.selectExpr( \
	"CONCAT(wrnty_clm.FQI_WRNTY_PFP_NB, '-', pfp_dm.pfp_ds) pfp_ds", \
	"wrnty_clm.dervd_vin_id as vin_id", \
	"wrnty_clm.VHCL_LN_YR_NB ", \
	"vhcl.nml_prdctn_mdl_cd ", \
	"CASE WHEN parent_team.cstmr_stsfcn_team_cd IS null then CONCAT(team.cstmr_stsfcn_team_cd, ' - ', team.cstmr_stsfcn_team_nm) ELSE CONCAT(parent_team.cstmr_stsfcn_team_cd, ' - ', parent_team.cstmr_stsfcn_team_nm) END AS rcvd_pfp_cstmr_stsfcn_team_ds", \
	"CASE WHEN parent_team.cstmr_stsfcn_team_cd IS null then null ELSE CONCAT(team.cstmr_stsfcn_team_cd, ' - ', team.cstmr_stsfcn_team_nm) END AS rcvd_pfp_cmpnt_cstmr_stsfcn_team_ds", \
	"trim(wrnty_clm.vhcl_sls_mdl_cd) as vhcl_sls_mdl_cd", \
	"trim(vhcl.vhcl_engn_srl_nb) as vhcl_engn_srl_nb", \
	"wrnty_clm.vhcl_clr_cd", \
	"vhcl.trm_cd ", \
	"vhcl.MNFCTG_VHCL_PLNT_CD ", \
	"vhcl.mnfctg_dt", \
	"vhcl.in_svc_dt", \
	"vhcl.glbl_mrkt_cd ", \
	"vhcl.POE_DSTBTN_FCLTY_CD ", \
	"wrnty_clm.cvrg_clas_cd", \
	"svc_dlr.dlr_rgn_cd as svc_dlr_rgn_nm", \
	"svc_dlr.dlr_st_cd as svc_dlr_st_nm", \
	"wrnty_clm.svc_dlr_nb", \
	"svc_dlr.dlr_nm as svc_dlr_nm", \
	"svc_dlr.dlr_phn_nb as svc_dlr_phn_nb", \
	"svc_dlr.svc_dstrct_cd", \
	"CAST(from_unixtime(unix_timestamp(wrnty_clm.VHCL_RPR_DT,  'MM/dd/yyyy HH:mm:ss.SSSSSS'), 'yyyy-MM-dd') AS DATE) as vhcl_rpr_dt ", \
	"wrnty_clm.dlr_rpr_ordr_nb ", \
	"regexp_replace(wrnty_clm.vhcl_mlg_nb, '(\\\.0*)', '') as vhcl_mlg_nb", \
	"wrnty_clm.wrnty_symptm_cd ", \
	"vhcl_symptm.symptm_ds AS wrnty_symptm_ds", \
	"wrnty_clm.wrnty_trbl_cd ", \
	"vhcl_trbl.trbl_ds ", \
	"cust_cmnt.wrnty_cmnt_tx AS cstmr_cmnt_tx", \
	"tech_cmnt.wrnty_cmnt_tx AS tchncn_cmnt_tx", \
	"wrnty_clm.dlr_lbr_am", \
	"wrnty_clm.dlr_prt_am", \
	"wrnty_clm.DSTRBR_CLM_NB", \
	"wrnty_clm.DLR_SBLT_AM", \
	"wrnty_clm.CLM_DSTRBR_LBR_AM", \
	"wrnty_clm.CLM_DSTRBR_PRT_AM", \
	"from_unixtime(unix_timestamp(vhcl.FCTRY_IVNTRY_TRNSFR_DT, 'MM/dd/yyyy HH:mm:ss.SSSS'), 'yyyy-MM-dd') as fctry_ivntry_trnsfr_dt", \
	"vhcl.TLMTCS_SVC_VNDR_CD", \
	"svc_dlr.DLR_PSTL_CD as SVC_DLR_PSTL_CD", \
	"vhcl.nci_vhcl_srs_cd", \
	"wrnty_clm.vhcl_lst_rplcd_km_nb", \
	"wrnty_clm.vhcl_lst_rplcd_mlg_nb", \
	"CONCAT(wrnty_clm.RCVD_PFP_1ST_5_NB,  '-',  pfp_dm.pfp_ds) rcvd_pfp_1st_5_ds", \
	"wrnty_clm.fqi_wrnty_pfp_nb", \
	"wrnty_clm.dlr_totl_am", \
	"vhcl.sold_flag AS sld_in", \
	"vhcl.vhcl_prdctn_src_cd", \
	"vhcl.tlmtcs_ind as tlmtcs_in", \
	"wrnty_clm.clm_net_am", \
	"wrnty_clm.clm_dstrbr_totl_am", \
	"vhcl.drv_trn_cd ", \
	"vhcl.vhcl_engn_mdl_cd ", \
	"vhcl.trnmsn_typ_cd ", \
	"vhcl.trnmsn_typ_nm ", \
	"vhcl.cv_in ", \
	"vhcl.vhcl_crshd_in ", \
	"vhcl.EMSN_CRTFCT_NB ", \
	"vhcl.emsn_typ_cd ", \
	"vhcl.vhcl_flt_in ", \
	"vhcl.ivntry_sts_cd ", \
	"trim(regexp_replace(vhcl.vhcl_yr_nb, '\\\.0', '')) vhcl_yr_nb", \
	"vhcl.NCI_IVNTRY_STS_CD ", \
	"vhcl.NCI_LCTN_STS_CD ", \
	"vhcl.plnt_cd_nm", \
	"vhcl.bdy_styl_nm ", \
	"VHCL.EIM_CD", \
	"vhcl.extr_clr_cd ", \
	"vhcl.extr_clr_nm ", \
	"vhcl.vhcl_ln_cd ", \
	"vhcl.vhcl_ln_nm ", \
	"vhcl.nci_optn_grp_cd ", \
	"vhcl.vhcl_nci_mdl_cd ", \
	"vhcl.nhtsa_vhcl_typ_cd ", \
	"TRIM(vhcl_nmc_mdl_cd) vhcl_nmc_mdl_cd", \
	"vhcl.trm_lvl_ds ", \
	"vhcl.fctry_shp_dt", \
	"vhcl.NCI_IVNTRY_ADDED_DT", \
	"VHCL.NMC_RCPT_DT", \
	"vhcl.orgnl_rtl_dt", \
	"vhcl.orgnl_in_svc_dt", \
	"VHCL.ORGNL_WHLSL_DT", \
	"VHCL.ORGNL_RTL_DLR_NB", \
	"vhcl.RTL_DLR_NB", \
	"vhcl.orgnl_rtl_dlr_cty_nm", \
	"vhcl.ORGNL_RTL_ST_CD", \
	"vhcl.rtl_dlr_cty_nm", \
	"vhcl.rtl_dlr_st_cd", \
	"vhcl.orgnl_whlsl_dlr_st_cd", \
	"VHCL.WHLSL_DLR_NB", \
	"vhcl.whlsl_dlr_st_cd", \
	"vhcl.INVC_DRFT_DT", \
	"vhcl.nci_orgnl_rtl_typ_cd", \
	"vhcl.orgnl_rtl_dlr_cntry_cd", \
	"vhcl.orgnl_rtl_dlr_st_cd", \
	"vhcl.orgnl_whlsl_dlr_cntry_cd", \
	"vhcl.orgnl_whlsl_dlr_nb", \
	"vhcl.orgnl_whlsl_dlr_cty_nm", \
	"vhcl.whlsl_dlr_cty_nm", \
	"vhcl.vhcl_nmc_mdl_nm ", \
	"vhcl.cntry_cd ", \
	"vhcl.vhcl_typ_cd ", \
	"vhcl.trm_cd_nm ", \
	"vhcl.bdy_styl_cd ", \
	"vhcl.prdctn_mdl_srs_cd ", \
	"vhcl.emsn_typ_nm ", \
	"vhcl.fctry_optns_tx ", \
	"vhcl.gnrc_optns_tx ", \
	"vhcl.vhcl_pltfrm_cd ", \
	"vhcl.mnfctg_dt_mnth_yr", \
	"vhcl.mnfctg_dt_yr AS mnfctg_yr_dt", \
	"vhcl.mnfctg_dt_day AS mnfctg_dy_dt", \
	"vhcl.in_svc_dt_mnth_yr AS in_svc_mnth_yr_dt", \
	"vhcl.rtl_sl_lse_dt", \
	"vhcl.orgnl_in_svc_dt_mnth_yr AS orgnl_in_svc_mnth_yr_dt", \
	"vhcl.engn_prfx_8_dgt_cd ", \
	"vhcl.vhcl_lctn_cd ", \
	"vhcl.vhcl_clsftn_cd ", \
	"vhcl.vhcl_clsftn_grp_cd ", \
	"vhcl.vhcl_mk_cd ", \
	"vhcl.vhcl_mk_nm", \
	"vhcl.vhcl_frgmt_in ", \
	"team.cstmr_stsfcn_team_cd AS cmnt_cstmr_stsfcn_team_cd", \
	"parent_team.cstmr_stsfcn_team_cd AS cstmr_stsfcn_team_cd", \
	"asgnd_engnr.rgn_engnr_nm ", \
	"wrnty_clm.blng_prcs_in ", \
	"wrnty_clm.clm_cvrg_cd ", \
	"wrnty_clm.clm_dstrbr_cd ", \
	"wrnty_clm.clm_typ_cd ", \
	"wrnty_clm.cvrg_itm_cd ", \
	"wrnty_clm.wrnty_btry_dgnstc_cd ", \
	"wrnty_clm.wrnty_bsns_typ_cd ", \
	"wrnty_clm.VHCL_HS_FL_RCRD_TYP_CD AS vhf_rcrd_typ_cd", \
	"wrnty_clm_sts_cd ", \
	"wrnty_clm.wrnty_aflt_rpt_exclsn_rfrnc_nb", \
	"wrnty_clm.plnt_asgnmt_cd ", \
	"wrnty_clm.rqstd_cvrg_cd ", \
	"wrnty_clm.rcvd_pfp_1st_5_nb ", \
	"wrnty_clm.WRNTY_ORGNL_PNC_ID", \
	"CAST(from_unixtime(unix_timestamp(wrnty_clm.clm_dt,  'MM/dd/yyyy HH:mm:ss.SSSSSS'), 'yyyy-MM-dd') AS DATE) clm_dt", \
	"CAST(from_unixtime(unix_timestamp(wrnty_clm.prts_rplc_dt,  'MM/dd/yyyy HH:mm:ss.SSSSSS'), 'yyyy-MM-dd') AS DATE) prts_rplc_dt", \
	"wrnty_clm.clm_fncl_dsbrmt_dt_nb ", \
	"from_unixtime(unix_timestamp(wrnty_clm.crdt_sm_rpt_dt,  'MM/dd/yyyy HH:mm:ss.SSSSSS'), 'yyyyMM') crdt_sts_rpt_yr_mnth_dt", \
	"CAST(from_unixtime(unix_timestamp(wrnty_clm.crdt_sm_rpt_dt,  'MM/dd/yyyy HH:mm:ss.SSSSSS'), 'yyyy-MM-dd') AS DATE) crdt_sm_rpt_dt", \
	"wrnty_clm.VHCL_KM_NB", \
	"CAST(from_unixtime(unix_timestamp(wrnty_clm.clm_wo_opn_dt,  'MM/dd/yyyy HH:mm:ss.SSSSSS'), 'yyyy-MM-dd') AS DATE) clm_wo_opn_dt ", \
	"wrnty_clm.AFLT_DLR_NB", \
	"wrnty_clm.src_splr_id ", \
	"wrnty_clm.glbl_splr_nb ", \
	"wrnty_clm.rcvd_pfp_splr_nb ", \
	"wrnty_clm.VHCL_RPR_ST_CD", \
	"wrnty_clm.VHCL_PRDCTN_MDL_SRS_CD", \
	"wrnty_clm.wrnty_pfp_nb ", \
	"wrnty_clm.rcvd_pfp_nb ", \
	"wrnty_clm.rpt_exclsn_in ", \
	"wrnty_clm.clm_bsns_typ_cd ", \
	"wrnty_clm.wrnty_clm_nb ", \
	"wrnty_clm.orgnl_symptm_cd ", \
	"wrnty_clm.orgnl_trbl_cd ", \
	"wrnty_clm.drvd_pfp_1st_5_nb ", \
	"wrnty_clm.jd_pwr_cd AS jdpwr_cd ", \
	"wrnty_clm.pnc_id ", \
	"wrnty_clm.svc_dy_cn ", \
	"wrnty_clm.pnc_typ_cd ", \
	"wrnty_clm.wrnty_clm_blng_cd ", \
	"wrnty_clm.PNC_CD AS prt_nm_cmpnt_cd", \
	"wrnty_clm.CLM_RCVRY_AM", \
	"wrnty_clm.CLM_DSTRBR_SBLT_AM", \
	"fqi_pfp.fqi_grp_cd ", \
	"FQI_GRP.fqi_grp_nm ", \
	"fqi_pfp.cnsdtd_pfp_nb", \
	"dstrbr.DSTRBR_NM", \
	"asgnd_engnr.RGN_NM", \
	"wrnty_clm.VHCL_PRDCTN_BLD_DT ", \
	"pnc_dm.pnc_nm ", \
	"fqi_rpsntv.rpsntv_nm as rspsbl_engnr_nm", \
	"wrnty_clm.DSTRBR_TOTL_ORGNL_CRNCY_AM ", \
	"CONCAT(wrnty_clm.wrnty_orgnl_pnc_id, ' - ', pnc_dm.pnc_nm) pnc_ds", \
	"asgnd_engnr.wrnty_engnr_nm ", \
	"CAST(from_unixtime(unix_timestamp(wrnty_clm.vhcl_in_svc_dt,  'MM/dd/yyyy HH:mm:ss.SSSSSS'), 'yyyy-MM-dd') AS DATE) vhcl_in_svc_dt", \
	"CAST(from_unixtime(unix_timestamp(wrnty_clm.sts_dt,  'MM/dd/yyyy HH:mm:ss.SSSSSS'), 'yyyy-MM-dd') AS DATE) sts_dt", \
	"vhcl.VHCL_MNFCTR_CMPNY_NM AS vhcl_mnfctr_cmpny_nm", \
	"CASE WHEN wrnty_clm.prts_rplc_dt is NULL THEN DATEDIFF(CAST(from_unixtime(unix_timestamp(wrnty_clm.vhcl_rpr_dt,  'MM/dd/yyyy HH:mm:ss.SSSSSS'), 'yyyy-MM-dd') AS DATE),   vhcl.orgnl_in_svc_dt) ELSE DATEDIFF(CAST(from_unixtime(unix_timestamp(wrnty_clm.vhcl_rpr_dt,  'MM/dd/yyyy HH:mm:ss.SSSSSS'), 'yyyy-MM-dd') AS DATE),   CAST(from_unixtime(unix_timestamp(wrnty_clm.prts_rplc_dt,  'MM/dd/yyyy HH:mm:ss.SSSSSS'), 'yyyy-MM-dd') AS DATE)) END as wrnty_dis_am", \
	"CASE WHEN substr(wrnty_clm.cvrg_itm_cd,  4,  7) = 'EXTN' THEN 'yes' ELSE 'no' END extnd_wrnty_in", \
	"to_date('1990-01-01') as wrnty_clm_mdfctn_dt", \
	"vhcl.btry_12_volt_srl_nb ", \
	"rcvd_splr.vndr_nm as rcvd_pfp_splr_nm", \
	"fncl_splr.vndr_nm as src_splr_nm", \
	"cvrg.cvrg_clas_ds as cvrg_clas_ds", \
	"'trd_cd' as trd_cd", \
	"wrnty_clm.crte_ts", \
	"GREATEST(CAST(pfp_dm.updt_ts as timestamp), CAST(rcvd_splr.updt_ts as timestamp), CAST(pnc_dm.updt_ts as timestamp), CAST(fqi_pfp.updt_ts as timestamp), CAST(fqi_crnt.updt_ts as timestamp), CAST(rspsbl_engnr_nm.updt_ts as timestamp), CAST(fqi_rpsntv.updt_ts as timestamp), CAST(FQI_GRP.updt_ts as timestamp), CAST(svc_dlr.ls_up_tm_stmp as timestamp), CAST(parent_team.updt_ts as timestamp), CAST(asgnd_engnr.updt_ts as timestamp), CAST(vhcl_symptm.updt_ts as timestamp), CAST(vhcl_trbl.updt_ts as timestamp), CAST(dstrbr.updt_ts as timestamp), CAST(wrnty_clm.updt_ts as timestamp),  CAST(vhcl.ls_up_tm_stmp as timestamp), CAST(team.updt_ts AS timestamp)) updt_ts")

	if filterQuery is not None:
		enrichedData = enrichedData.filter(filterQuery)

	return enrichedData.alias('enrichedData')

# Second half of the final table. To add incremental upload after all tables have timestamps
def getServiceOperationData(spark, filterQuery = None):
	# TODO: Comment why we do a max on original labor operation code
	serviceOperations = spark.read.table(database + ".SVC_OPRTN_CRNT_VW")
	filteredServiceOperations = serviceOperations.filter(serviceOperations['svc_oprtn_prmry_in']).alias("svc_oprtn")
	laborOperations = spark.read.table(database + ".LBR_OPRTN_CRNT_VW").alias("lbr_prim")
	
	serviceOperationInfo = filteredServiceOperations.join(laborOperations, filteredServiceOperations['orgnl_lbr_oprtn_cd'] == laborOperations['lbr_oprtn_cd'], 'left_outer') \
				.groupBy(filteredServiceOperations['wrnty_clm_nb'], filteredServiceOperations['svc_oprtn_prmry_in']).agg(\
					max(filteredServiceOperations['orgnl_lbr_oprtn_cd']).alias('max_orgnl_lbr_oprtn_cd'), \
					max(filteredServiceOperations['lbr_oprtn_cd']).alias('prmry_lbr_oprtn_cd'), \
					max(filteredServiceOperations['orgnl_lbr_oprtn_cd']).alias('prmry_orgnl_lbr_oprtn_cd'), \
					collect_list(laborOperations['lbr_oprtn_nm']).alias('orgnl_lbr_oprtn_nm'), \
					greatest(max(laborOperations['updt_ts']), max(filteredServiceOperations['updt_ts'])).alias('updt_ts')) \
				.withColumnRenamed('wrnty_clm_nb', 'svc_oprtn_wrnty_clm_nb')

	if filterQuery is not None:
		serviceOperationInfo = serviceOperationInfo.filter(filterQuery)

	return serviceOperationInfo.alias('serviceOperationInfo')

def getOriginalOperationData(spark, filterQuery = None):
	operations = spark.read.table(database + ".SVC_OPRTN_CRNT_VW").withColumnRenamed('lbr_oprtn_cd', 'oprtn_lbr_oprtn_cd').alias("oprtn")
	labor = spark.read.table(database + ".LBR_OPRTN_CRNT_VW").alias("lbr")
	originalLaborOperations = spark.read.table(database + ".LBR_OPRTN_CRNT_VW").withColumnRenamed('lbr_oprtn_cd', 'original_lbr_oprtn_cd').withColumnRenamed('lbr_oprtn_nm', 'original_lbr_oprtn_nm').alias("orgnl_lbr")
	operationLocations = spark.read.table(database + ".oprtn_lctn_crnt_vw").alias("lctn")
	operationInfo = operations.join(labor, operations['oprtn_lbr_oprtn_cd'] == labor['lbr_oprtn_cd'], 'left_outer') \
					.join(broadcast(operationLocations), operationLocations['oprtn_lctn_cd'] == substring(operations['oprtn_lbr_oprtn_cd'], 2, 2), 'left_outer') \
					.join(originalLaborOperations, [originalLaborOperations['original_lbr_oprtn_cd'] == operations['orgnl_lbr_oprtn_cd'], ~operations['svc_oprtn_prmry_in']], 'left_outer') \
					.groupBy(operations['wrnty_clm_nb']).agg( \
						collect_list(operations['oprtn_lbr_oprtn_cd']).alias('lbr_oprtn_cd'), \
						collect_list(operations['lbr_hr_nb']).alias('lbr_hr_nb'), \
						sum(operations['lbr_hr_nb']).alias('totl_lbr_hr_nb'), \
						collect_list(operationLocations['oprtn_lctn_ds']).alias('oprtn_lctn_ds'), \
	                                        collect_list(operationLocations['oprtn_lctn_cd']).alias('oprtn_lctn_cd'), \
						collect_list(concat(operations['oprtn_lbr_oprtn_cd'], lit('-'), operations['lbr_hr_nb'], lit('-'), labor['lbr_oprtn_nm'])).alias('lbr_oprtn_hr_nm'), \
						collect_list(originalLaborOperations['original_lbr_oprtn_nm']).alias('orgnl_lbr_oprtn_ds'), \
						greatest(max(operationLocations['updt_ts']), max(originalLaborOperations['updt_ts']), max(labor['updt_ts']), max(operations['updt_ts'])).alias('updt_ts')) \
					.withColumnRenamed('wrnty_clm_nb', 'oprtn_wrnty_clm_nb')
	
	if filterQuery is not None:
		operationInfo = operationInfo.filter(filterQuery)

	return operationInfo.alias('operationInfo')
	
def getPartsData(spark, filterQuery = None):
	# Seems as though there will always only be one value for pfp_nb_ds. Should only be one row per warranty claim number, and one row per PFP number?
	# Maybe this should be moved up into the scalar join above.
	partsWarrantyClaims = spark.read.table(database + ".wrnty_clm_crnt_vw").alias("parts_wrnty_clm")
	serviceParts = spark.read.table(database + '.svc_prt_crnt_vw').alias('svc_prt')
	serviceParts=serviceParts.withColumn("prt_cst_am", serviceParts["prt_cst_am"].cast(DecimalType(8,2)))
	parts = spark.read.table(database + '.prt_crnt_vw').alias('part')
	partsPFP = spark.read.table(database + '.pfp_crnt_vw').alias('partsPFP')
	partInfo = partsWarrantyClaims.join(serviceParts, serviceParts['wrnty_clm_nb'] == partsWarrantyClaims['wrnty_clm_nb'], 'left_outer') \
					.join(parts, serviceParts['prt_nb'] == parts['prt_nb'], 'left_outer') \
					.join(broadcast(partsPFP), partsWarrantyClaims['wrnty_pfp_nb'] == partsPFP['pfp_nb'], 'left_outer') \
					.groupBy(partsWarrantyClaims['wrnty_clm_nb']).agg(\
						collect_list(regexp_replace(serviceParts['prt_qt'], r'(\\\.0*)', '')).alias('prt_qt'), \
						collect_list(serviceParts['prt_nb']).alias('prt_nb'), \
						collect_list(parts['prt_nm']).alias('pfp_nb_ds'), \
						collect_list(concat(serviceParts['prt_nb'], lit(' - '), serviceParts['prt_cst_am'], lit(' - '), regexp_replace(serviceParts['prt_qt'], r'(\\\.0*)', ''), lit(' - '), parts['prt_nm'])).alias('svc_prt_cst_qt_ds'), \
						greatest(max(partsWarrantyClaims['updt_ts']), max(serviceParts['updt_ts']), max(partsPFP['updt_ts']), max(parts['updt_ts'])).alias('updt_ts')) \
					.withColumnRenamed('wrnty_clm_nb', 'prt_wrnty_clm_nb')

	if filterQuery is not None:
		partInfo = partInfo.filter(filterQuery)

	return partInfo.alias('partInfo')

def insertRows(enrichedData, serviceOperationInfo, operationInfo, partInfo, filterQuery = None):
	# Final table. To change it to incremental update after the second table is changed to incremental.
	insertableData = enrichedData.join(serviceOperationInfo, enrichedData['wrnty_clm_nb'] == serviceOperationInfo['svc_oprtn_wrnty_clm_nb'], 'left_outer') \
					.join(operationInfo, enrichedData['wrnty_clm_nb'] == operationInfo['oprtn_wrnty_clm_nb'], 'left_outer') \
					.join(partInfo, enrichedData['wrnty_clm_nb'] == partInfo['prt_wrnty_clm_nb'], 'left_outer') \

	if filterQuery is not None:
		insertableData = insertableData.filter(filterQuery)

	insertableData = insertableData.selectExpr( \
						"enrichedData.wrnty_clm_nb", \
						"serviceOperationInfo.max_orgnl_lbr_oprtn_cd", \
						"IF(operationInfo.lbr_oprtn_cd IS NULL, array(), operationInfo.lbr_oprtn_cd) AS lbr_oprtn_cd", \
						"IF(operationInfo.lbr_hr_nb IS NULL, array(), operationInfo.lbr_hr_nb) AS lbr_hr_nb", \
						"IF(partInfo.prt_qt IS NULL, array(), partInfo.prt_qt) AS prt_qt", \
						"IF(partInfo.prt_nb IS NULL, array(), partInfo.prt_nb) AS prt_nb", \
						"serviceOperationInfo.prmry_lbr_oprtn_cd", \
						"serviceOperationInfo.prmry_orgnl_lbr_oprtn_cd", \
						"serviceOperationInfo.svc_oprtn_prmry_in", \
						"IF(partInfo.pfp_nb_ds IS NULL, array(), partInfo.pfp_nb_ds) AS pfp_nb_ds", \
						"IF(partInfo.svc_prt_cst_qt_ds IS NULL, array(), partInfo.svc_prt_cst_qt_ds) AS svc_prt_cst_qt_ds", \
						"IF(serviceOperationInfo.orgnl_lbr_oprtn_nm IS NULL, array(), serviceOperationInfo.orgnl_lbr_oprtn_nm) AS orgnl_lbr_oprtn_nm", \
						"IF(operationInfo.oprtn_lctn_ds IS NULL, array(), operationInfo.oprtn_lctn_ds) AS oprtn_lctn_ds", \
						"IF(operationInfo.oprtn_lctn_cd IS NULL, array(), operationInfo.oprtn_lctn_cd) AS oprtn_lctn_cd", \
						"IF(operationInfo.lbr_oprtn_hr_nm IS NULL, array(), operationInfo.lbr_oprtn_hr_nm) AS lbr_oprtn_hr_nm", \
						"IF(operationInfo.orgnl_lbr_oprtn_ds IS NULL, array(), operationInfo.orgnl_lbr_oprtn_ds) AS orgnl_lbr_oprtn_ds", \
						"enrichedData.pfp_ds", \
						"enrichedData.vin_id", \
						"enrichedData.vhcl_ln_yr_nb", \
						"enrichedData.nml_prdctn_mdl_cd", \
						"enrichedData.rcvd_pfp_cstmr_stsfcn_team_ds", \
						"enrichedData.rcvd_pfp_cmpnt_cstmr_stsfcn_team_ds", \
						"enrichedData.vhcl_sls_mdl_cd", \
						"enrichedData.vhcl_engn_srl_nb", \
						"enrichedData.vhcl_clr_cd", \
						"enrichedData.trm_cd", \
						"enrichedData.mnfctg_vhcl_plnt_cd", \
						"enrichedData.mnfctg_dt", \
						"enrichedData.in_svc_dt", \
						"enrichedData.glbl_mrkt_cd", \
						"enrichedData.poe_dstbtn_fclty_cd", \
						"enrichedData.cvrg_clas_cd", \
						"enrichedData.svc_dlr_rgn_nm", \
						"enrichedData.svc_dlr_st_nm", \
						"enrichedData.svc_dlr_nb", \
						"enrichedData.svc_dlr_nm", \
						"enrichedData.svc_dlr_phn_nb", \
						"enrichedData.vhcl_rpr_dt", \
						"enrichedData.dlr_rpr_ordr_nb", \
						"enrichedData.vhcl_mlg_nb", \
						"enrichedData.wrnty_symptm_cd", \
						"enrichedData.wrnty_symptm_ds", \
						"enrichedData.wrnty_trbl_cd", \
						"enrichedData.trbl_ds", \
						"enrichedData.cstmr_cmnt_tx", \
						"enrichedData.tchncn_cmnt_tx", \
						"operationInfo.totl_lbr_hr_nb", \
						"enrichedData.dlr_lbr_am", \
						"enrichedData.dlr_prt_am", \
						"enrichedData.DSTRBR_CLM_NB", \
						"enrichedData.DLR_SBLT_AM", \
						"enrichedData.CLM_DSTRBR_LBR_AM", \
						"enrichedData.CLM_DSTRBR_PRT_AM", \
						"enrichedData.FCTRY_IVNTRY_TRNSFR_DT", \
						"enrichedData.TLMTCS_SVC_VNDR_CD", \
						"enrichedData.SVC_DLR_PSTL_CD", \
						"enrichedData.nci_vhcl_srs_cd", \
						"enrichedData.vhcl_lst_rplcd_km_nb", \
						"enrichedData.vhcl_lst_rplcd_mlg_nb", \
						"enrichedData.rcvd_pfp_1st_5_ds", \
						"enrichedData.fqi_wrnty_pfp_nb", \
						"enrichedData.dlr_totl_am", \
						"enrichedData.sld_in", \
						"enrichedData.vhcl_prdctn_src_cd", \
						"enrichedData.tlmtcs_in", \
						"enrichedData.clm_net_am", \
						"enrichedData.clm_dstrbr_totl_am", \
						"enrichedData.drv_trn_cd", \
						"enrichedData.vhcl_engn_mdl_cd", \
						"enrichedData.trnmsn_typ_cd", \
						"enrichedData.trnmsn_typ_nm", \
						"enrichedData.cv_in", \
						"enrichedData.vhcl_crshd_in", \
						"enrichedData.emsn_crtfct_nb", \
						"enrichedData.emsn_typ_cd", \
						"enrichedData.vhcl_flt_in", \
						"enrichedData.ivntry_sts_cd", \
						"enrichedData.vhcl_yr_nb", \
						"enrichedData.nci_ivntry_sts_cd", \
						"enrichedData.nci_lctn_sts_cd", \
						"enrichedData.plnt_cd_nm", \
						"enrichedData.bdy_styl_nm", \
						"enrichedData.eim_cd", \
						"enrichedData.extr_clr_cd", \
						"enrichedData.extr_clr_nm", \
						"enrichedData.vhcl_ln_cd", \
						"enrichedData.vhcl_ln_nm", \
						"enrichedData.nci_optn_grp_cd", \
						"enrichedData.vhcl_nci_mdl_cd", \
						"enrichedData.nhtsa_vhcl_typ_cd", \
						"enrichedData.vhcl_nmc_mdl_cd", \
						"enrichedData.trm_lvl_ds", \
						"enrichedData.fctry_shp_dt", \
						"enrichedData.nci_ivntry_added_dt", \
						"enrichedData.nmc_rcpt_dt", \
						"enrichedData.orgnl_rtl_dt", \
						"enrichedData.orgnl_in_svc_dt", \
						"enrichedData.orgnl_whlsl_dt", \
						"enrichedData.orgnl_rtl_dlr_nb", \
						"enrichedData.rtl_dlr_nb", \
						"enrichedData.orgnl_rtl_dlr_cty_nm", \
						"enrichedData.orgnl_rtl_st_cd", \
						"enrichedData.rtl_dlr_cty_nm", \
						"enrichedData.rtl_dlr_st_cd", \
						"enrichedData.orgnl_whlsl_dlr_st_cd", \
						"enrichedData.whlsl_dlr_nb", \
						"enrichedData.whlsl_dlr_st_cd", \
						"enrichedData.invc_drft_dt", \
						"enrichedData.nci_orgnl_rtl_typ_cd", \
						"enrichedData.orgnl_rtl_dlr_cntry_cd", \
						"enrichedData.orgnl_rtl_dlr_st_cd", \
						"enrichedData.orgnl_whlsl_dlr_cntry_cd", \
						"enrichedData.orgnl_whlsl_dlr_nb", \
						"enrichedData.orgnl_whlsl_dlr_cty_nm", \
						"enrichedData.whlsl_dlr_cty_nm", \
						"enrichedData.vhcl_nmc_mdl_nm", \
						"enrichedData.cntry_cd", \
						"enrichedData.vhcl_typ_cd", \
						"enrichedData.trm_cd_nm", \
						"enrichedData.bdy_styl_cd", \
						"enrichedData.prdctn_mdl_srs_cd", \
						"enrichedData.emsn_typ_nm", \
						"enrichedData.fctry_optns_tx", \
						"enrichedData.gnrc_optns_tx", \
						"enrichedData.vhcl_pltfrm_cd", \
						"enrichedData.mnfctg_dt_mnth_yr", \
						"enrichedData.mnfctg_yr_dt", \
						"enrichedData.mnfctg_dy_dt", \
						"enrichedData.in_svc_mnth_yr_dt", \
						"enrichedData.rtl_sl_lse_dt", \
						"enrichedData.orgnl_in_svc_mnth_yr_dt", \
						"enrichedData.engn_prfx_8_dgt_cd", \
						"enrichedData.vhcl_lctn_cd", \
						"enrichedData.vhcl_clsftn_cd", \
						"enrichedData.vhcl_clsftn_grp_cd", \
						"enrichedData.vhcl_mk_cd", \
						"enrichedData.vhcl_mk_nm", \
						"enrichedData.vhcl_frgmt_in", \
						"enrichedData.cmnt_cstmr_stsfcn_team_cd", \
						"enrichedData.cstmr_stsfcn_team_cd", \
						"enrichedData.rgn_engnr_nm", \
						"enrichedData.blng_prcs_in", \
						"enrichedData.clm_cvrg_cd", \
						"enrichedData.clm_dstrbr_cd", \
						"enrichedData.clm_typ_cd", \
						"enrichedData.cvrg_itm_cd", \
						"enrichedData.wrnty_btry_dgnstc_cd", \
						"enrichedData.wrnty_bsns_typ_cd", \
						"enrichedData.vhf_rcrd_typ_cd", \
						"enrichedData.wrnty_clm_sts_cd", \
						"enrichedData.wrnty_aflt_rpt_exclsn_rfrnc_nb", \
						"enrichedData.plnt_asgnmt_cd", \
						"enrichedData.rqstd_cvrg_cd", \
						"enrichedData.rcvd_pfp_1st_5_nb", \
						"enrichedData.wrnty_orgnl_pnc_id", \
						"enrichedData.clm_dt", \
						"enrichedData.prts_rplc_dt", \
						"enrichedData.clm_fncl_dsbrmt_dt_nb", \
						"enrichedData.crdt_sts_rpt_yr_mnth_dt", \
						"enrichedData.crdt_sm_rpt_dt", \
						"enrichedData.vhcl_km_nb", \
						"enrichedData.clm_wo_opn_dt", \
						"enrichedData.aflt_dlr_nb", \
						"enrichedData.src_splr_id", \
						"enrichedData.glbl_splr_nb", \
						"enrichedData.rcvd_pfp_splr_nb", \
						"enrichedData.vhcl_rpr_st_cd", \
						"enrichedData.vhcl_prdctn_mdl_srs_cd", \
						"enrichedData.wrnty_pfp_nb", \
						"enrichedData.rcvd_pfp_nb", \
						"enrichedData.rpt_exclsn_in", \
						"enrichedData.clm_bsns_typ_cd", \
						"enrichedData.orgnl_symptm_cd", \
						"enrichedData.orgnl_trbl_cd", \
						"enrichedData.drvd_pfp_1st_5_nb", \
						"enrichedData.jdpwr_cd", \
						"enrichedData.pnc_id", \
						"enrichedData.svc_dy_cn", \
						"enrichedData.pnc_typ_cd", \
						"enrichedData.wrnty_clm_blng_cd", \
						"enrichedData.prt_nm_cmpnt_cd", \
						"enrichedData.clm_rcvry_am", \
						"enrichedData.fqi_grp_cd", \
						"enrichedData.fqi_grp_nm", \
						"enrichedData.cnsdtd_pfp_nb", \
						"enrichedData.dstrbr_nm", \
						"enrichedData.rgn_nm", \
						"enrichedData.vhcl_prdctn_bld_dt", \
						"enrichedData.pnc_nm", \
						"enrichedData.rspsbl_engnr_nm", \
						"enrichedData.dstrbr_totl_orgnl_crncy_am", \
						"enrichedData.pnc_ds", \
						"enrichedData.wrnty_engnr_nm", \
						"enrichedData.vhcl_in_svc_dt", \
						"enrichedData.sts_dt", \
						"enrichedData.vhcl_mnfctr_cmpny_nm", \
						"enrichedData.wrnty_dis_am", \
						"enrichedData.extnd_wrnty_in", \
						"enrichedData.wrnty_clm_mdfctn_dt", \
						"enrichedData.btry_12_volt_srl_nb", \
						"enrichedData.rcvd_pfp_splr_nm", \
						"enrichedData.src_splr_nm", \
						"enrichedData.cvrg_clas_ds", \
						"enrichedData.trd_cd", \
						"'" + currentUser + "' crte_usr_id", \
						"enrichedData.crte_ts", \
						"'" + currentUser + "' updt_usr_id", \
						"from_unixtime(unix_timestamp()) updt_ts", \
						"enrichedData.clm_dstrbr_sblt_am", \
						"enrichedData.svc_dstrct_cd") 
	
	insertableData.write.insertInto(database + '.wrnty_clm_vhcl_solr', overwrite = False)


database = sys.argv[1]

spark = SparkSession.builder.appName("Warranty Claims Vehicle Daily Load").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel('WARN')

currentUser = spark.sparkContext.sparkUser()

lastUpdateTimestamp = spark.sql("select max(updt_ts) from " + database + ".wrnty_clm_vhcl_solr").rdd.flatMap(lambda x: x).first()

if lastUpdateTimestamp is None:
        lastUpdateTimestamp = '1990-01-01 00:00:00'

wrnty_solr_updt_decision=spark.sql("select CASE when max(wc.updt_ts) > CAST('"+str(lastUpdateTimestamp)+"' as timestamp) then 'proceed' else 'no' end as decision from equip.wrnty_clm_base wc").rdd.flatMap(lambda x : x).first()

if(str(wrnty_solr_updt_decision) == 'proceed'):
	enrichmentInfo = getEnrichedData(spark)
	serviceOperationInfo = getServiceOperationData(spark)
	operationInfo = getOriginalOperationData(spark)
	partInfo = getPartsData(spark)

	insertRows(enrichmentInfo, serviceOperationInfo, operationInfo, partInfo, "GREATEST(enrichedData.updt_ts, serviceOperationInfo.updt_ts, operationInfo.updt_ts, partInfo.updt_ts) > CAST('" + str(lastUpdateTimestamp) + "' AS TIMESTAMP)")

else:
	print("Warranty app table is up to date at this time.")
spark.stop()

