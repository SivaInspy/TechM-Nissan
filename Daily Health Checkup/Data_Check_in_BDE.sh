#!/bin/bash
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM dbsro.repair_order_history group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM dbsro.repair_order_line_history group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.pfp_engnr group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.cmpnt_grp group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.cstmr_stsfcn_team group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.fld_engrng_stf group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.fld_qlty_role group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.fqi_grp group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.fqi_pfp group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.fqi_rpsntv group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.cstmr_stsfcn_team_mbr group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.pfp_rspsbl_engnr group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.fqi_base_mdl group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.fqi_crnt_mdl group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.ir_cmbnd_vhcl_clm_lmtd_flds_cmltv group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs_cmltv group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs_vms_cms group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.ir_cmbnd_vhcl_clm_lmtd_flds_vms_cms group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.ir_cmbnd_vhcl_clm_lmtd_flds_dflt_fltrs group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.ir_cmbnd_vhcl_clm_lmtd_flds group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.aves_base group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.aves_vhcl_solr group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM drive.CMDTA_BASE group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM drive.PRTS_DTL_CMDTA_BASE group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM drive.PLNT_CD_ARAY_BASE group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM drive.HS_BASE group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM drive.RQST_PRT_MVMNT_BASE group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM drive.ADTNL_PFP_ARAY_BASE group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM drive.MDL_CD_ARAY_BASE group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM drive.BIC_BASE group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM drive.DPRTMT_BASE group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM drive.DSGN_NT_CMDTA_BASE group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM drive.DSGN_NT_ADR_BASE group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM drive.USR_INFRMN_BASE group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM drive.USR_ROLE_BASE group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM drive.VHCL_BASE group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM drive.RQST_BASE group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM drive.ADR_BASE group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM drive.MDL_YR_ARAY_BASE group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM drive.HRZNTL_DPLYMT_MDL_ARAY_BASE group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM drive.PRJCT_BASE group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM drive.ADPTN_DATA_BASE group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM drive.MRKT_RPLY_PBLC_BASE group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM drive.WRKFLW_BASE group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM drive.MRKT_RPLY_BASE group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM drive.TECH_RPT_BASE group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM odi.nhtsa_ivstgn_base group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM odi.nhtsa_rcl_base group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM odi.nhtsa_svc_bltn_base group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM odi.nhtsa_cnsmr_cmplnt_base group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM rcl.rcl_base group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM rcl.vhcl_rcl_base group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(repair_date) as day, count(*) as recordscount FROM equip.adhoc_claim_dm group by DATE(repair_date) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(reportdate) as day, count(*) as recordscount FROM equip.adhoc_claim group by DATE(reportdate) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(rundate) as day, count(*) as recordscount FROM equip.adhoc_comment_cust group by DATE(rundate) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(rundate) as day, count(*) as recordscount FROM equip.adhoc_comment_tech group by DATE(rundate) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(processdate) as day, count(*) as recordscount FROM equip.adhoc_cpv group by DATE(processdate) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM equip.adhoc_gcar_project group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM equip.adhoc_gcar_req group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(reportdate) as day, count(*) as recordscount FROM equip.adhoc_gcarmaster group by DATE(reportdate) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(update_dt) as day, count(*) as recordscount FROM equip.adhoc_opcode group by DATE(update_dt) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(update_dt) as day, count(*) as recordscount FROM equip.adhoc_part group by DATE(update_dt) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(reportdate) as day, count(*) as recordscount FROM equip.adhoc_tracking_CVTClaims_F1 group by DATE(reportdate) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(reportdate) as day, count(*) as recordscount FROM equip.adhoc_tracking_CVTClaims_FKJK group by DATE(reportdate) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(update_dt) as day, count(*) as recordscount FROM equip.adhoc_tracking_CVTPartUse_F1 group by DATE(update_dt) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(update_dt) as day, count(*) as recordscount FROM equip.adhoc_tracking_CVTPartUse_FKJK group by DATE(update_dt) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(reportdate) as day, count(*) as recordscount FROM equip.adhoc_vehicle group by DATE(reportdate) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(rundate) as day, count(*) as recordscount FROM equip.adhoc_vehicle_gcar_conspfp_cov group by DATE(rundate) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(rundate) as day, count(*) as recordscount FROM equip.adhoc_vehicle_navi group by DATE(rundate) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(opcode_update_dt) as day, count(*) as recordscount FROM equip.adhoc_vehicleopcode group by DATE(opcode_update_dt) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(part_update_dt) as day, count(*) as recordscount FROM equip.adhoc_vehiclepart group by DATE(part_update_dt) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM vhcl_qlty_srvy.nsn_trbl_base group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM vhcl_qlty_srvy.nsn_trbl_rspsbl_engnr_base group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM vhcl_qlty_srvy.qcs_adtnl_rspns_base group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM vhcl_qlty_srvy.qcs_ck_bx_base group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM vhcl_qlty_srvy.qcs_rspndt_base group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(crte_ts) as day, count(*) as recordscount FROM vhcl_qlty_srvy.qcs_rspns_base group by DATE(crte_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.cnsmr_afr_cmnt_base group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.cnsmr_afr_base group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.cnsmr_afr_trd_cncrn_base group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.cnsmr_afr_vhcl_solr group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.tchln_base group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.tchln_cmnt_base group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.tchln_trd_base group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.tchln_vhcl_solr group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.inspct_base group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.inspct_vhcl_solr group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.WRNTY_CVRG_CLAS_BASE group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.ASGND_ENGNR_BASE group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.DSTRBR_BASE group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.SVC_OPRTN_BASE group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.SVC_PRT_BASE group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.WRNTY_CMNT_BASE group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.PNC_BASE group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.PFP_BASE group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.VHCL_SYMPTM_BASE group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.VHCL_TRBL_BASE group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.WRNTY_CLM_BASE group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.WRNTY_BSNS_TYP_BASE group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.VNDR_BASE group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.VHCL_ENGN_MDL_BASE group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.PRT_BASE group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.PRDCTN_MDL_SRS_BASE group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.OPRTN_LCTN_BASE group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.MDPRT_ASGNMT_BASE group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.LBR_OPRTN_BASE group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.GLBL_SPLR_BASE group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.wrnty_clm_vhcl_solr group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(updt_ts) as day, count(*) as recordscount FROM equip.mileage group by DATE(updt_ts) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(ls_up_tm_stmp) as day, count(*) as recordscount FROM equip.vhcl group by DATE(ls_up_tm_stmp) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(ls_up_tm_stmp) as day, count(*) as recordscount FROM equip.DLR_DM group by DATE(ls_up_tm_stmp) order by day desc limit 1;" >> input.hql
echo "set hive.cli.print.header=true;SELECT DATE(ls_up_tm_stmp) as day, count(*) as recordscount FROM equip.vhcl_solr group by DATE(ls_up_tm_stmp) order by day desc limit 1;" >> input.hql

date1=`date -d "1 day ago" '+%Y-%m-%d'`
date2=`date -d "2 day ago" '+%Y-%m-%d'`

hive -f input.hql > input.txt

while IFS="\t" read -r line1 line2; do
    if [[ $line1==$date1 ]];
    then
      echo "set hive.cli.print.header=true;Data loaded for Yesterday $date1" >> date1.txt
    elif [[ $line1==$date2 ]];
    then
      echo "set hive.cli.print.header=true;Data loaded for Day before Yesterday $date2" >> date2.txt
    fi
done < input.txt

cat=`cat input.txt | grep $date | wc -l`
if [[ $cat -lt 2 ]]
then
#echo -e "Hi All,\n\nDBSRO ro and ro line history records are not loaded today \n\nRegards,\nDbsro data alert system\n" | mail -s "[BDE-PRD] Failed DBSRO-data-alert-BDE-PRD-wf" ISEQUIPBDE@Nissan-USA.com
fi

rm -r input.txt
