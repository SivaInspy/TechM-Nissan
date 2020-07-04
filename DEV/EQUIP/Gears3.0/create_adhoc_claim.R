###################################################################
#  Creating Adhoc Claim
#  Created by: Quiton, J
#  Modified by: Quiton, J
#  Update date: 2019.04.24
#               Expanded cmis indicator to 60
#####################################################################

#  HIVE tuning parameter options
   bucket.one<-1
   bucket.tiny<-4
   bucket.small<-60
  bucket.medium<-120
   bucket.large<-240
  bucket.xlarge<-480
 bucket.xxlarge<-960
 bucket.xxxlarge<-1200
 rows_per_bucket<-36000
  bseq<-1:60*40
 bucket_vec<-c(1,2,4,10,bseq)


##########################
# GLOBAL PARAMETERS
###########################
CREATE_TABLE_FLAG=TRUE
db_name<-"EQUIP"
db_drive_source<-"DRIVE"
drive_project<-"prjct_base"
drive_req<-"rqst_base"
drive_vb<-"vhcl_base"
drive_adptn<-"adptn_data_base"
drive_adr<-"adr_base"
drive_cmdta<-"cmdta_base"
drive_model<-"mdl_cd_aray_base"
drive_pfp<-"adtnl_pfp_aray_base"
raw_prefix<-paste(db_name,".adhoc_raw_",sep="")
adhoc_prefix<-paste(db_name,".adhoc_",sep="")
spark_yarn_app_name_ <- 'sparkr-create_adhoc_claim'
loglevel <- 'ERROR'

###############################################################
#  Task 00: CREATE SPARK SESSION
#################################################################
build_SparkR_environment_ <- function(spark_app_name, loglevel, queue="ad-hoc") {

  # Set the Spark environment, parameters & variables
  spark_yarn_app_name_ <- spark_app_name
  spark_home_ <- "/usr/hdp/current/spark2-client/"

  if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
    Sys.setenv(SPARK_HOME = spark_home_)
  }

  library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
  sparkR.session( master = "yarn",
                  appName = spark_yarn_app_name_,
                  sparkHome = Sys.getenv("SPARK_HOME"),
                  sparkConfig=list(spark.yarn.queue= queue,
                                   spark.sql.crossJoin.enabled='TRUE'),
                  enableHiveSupport = TRUE )

  setLogLevel(loglevel) # Set the log level
  return(0)
}

status_ <- build_SparkR_environment_( spark_yarn_app_name_, loglevel = loglevel, queue="ad-hoc" )

txt<-paste('use ', db_name,sep='')
sql(txt)
db_list<-collect(sql('show tables'))$tableName

txt<-paste(' set spark.sql.hive.filesourcePartitionFileCacheSize= ', 2*1024^3)
collect(sql(txt))

#####################################
# Task 01: Define functions
#####################################
insert_overwrite<-function(bucket, partition, hivetable, temptable,
                           whereclause="", clusterclause=""){

   txt<-paste('set spark.sql.shuffle.partitions=', bucket, sep='')
   sql(txt)

   temp<-sql(paste('select * from ',temptable, ' limit 1',sep=''))
   hive<-sql(paste("select * from ",hivetable, ' limit 1',sep=''))

   partition_txt<-ifelse(partition=="","",   paste(" PARTITION (", partition,") ",sep=""))
   where_txt<-ifelse(whereclause=="","",   paste(" WHERE ", whereclause,sep=""))
   cluster_txt<-ifelse(clusterclause=="","",   paste(" CLUSTER BY ", clusterclause,sep=""))

   tf<-ifelse(sum(colnames(temp) %in% "TableFlag")>0, "", "1 as TableFlag,")
   v_obj<-colnames(hive)
   idx<-grep("TableFlag",v_obj)
   if (min(idx)>0){
       v_obj<-v_obj[-idx]
    }

   v_obj<-paste(v_obj,collapse="`,`")
   v_obj<-paste(tf,"`",v_obj,"`",sep="")

    txt<-paste(" INSERT OVERWRITE TABLE ", hivetable, partition_txt,
               " SELECT ", v_obj, " from ", temptable, " a ", where_txt, cluster_txt, sep="")
   txt=gsub('\n', ' ',txt)
   txt<-gsub('\\s+', ' ',txt)
   sql(txt)
}


create_hive<-function(bucket, partition, hivetable, temptable,
             whereclause="",
             clusterclause=""){
   txt<-paste("set spark.sql.shuffle.partitions=",bucket,sep="")
   sql(txt)

   txt<-paste("DROP TABLE IF EXISTS ", hivetable,sep="")
   sql(txt)
   v_obj=" a.* "

   temp<-sql(paste('select * from ',temptable, ' limit 1',sep=''))

   tf<-ifelse(sum(colnames(temp) %in% "TableFlag")>0, "", "1 as TableFlag,")

   partition_txt<-ifelse(partition=="","",   paste(" PARTITIONED BY (", partition,") ",sep=""))
   where_txt<-ifelse(whereclause=="","",   paste(" WHERE ", whereclause,sep=""))
   cluster_txt<-ifelse(clusterclause=="","",   paste(" CLUSTER BY ", clusterclause,sep=""))

   txt<-paste(" CREATE TABLE ", hivetable, " USING ORC ", partition_txt,
                " as SELECT ", tf, v_obj, " from  ", temptable, " a ", where_txt, cluster_txt, sep="")
   txt=gsub('\n', ' ',txt)
   txt<-gsub('\\s+', ' ',txt)

   sql(txt)
}

persist_table<-function(db, tablename,cacheFlag=FALSE){
    createOrReplaceTempView(db,tablename)
   if (cacheFlag==TRUE){
     uncacheTable(tablename)
      cacheTable(tablename)
   }
   return(tablename)
}

#############################################
# Step 1: Create warranty comments table
#############################################
v_obj<-' CURRENT_DATE() as rundate, wrnty_clm_nb as cust_clm_nb, trim(lower(wrnty_cmnt_tx)) as cust_comment '
v_group<-' CURRENT_DATE(), wrnty_clm_nb, trim(lower(wrnty_cmnt_tx))'
v_table<-paste(raw_prefix, 'wrnty_cmnt_crnt_vw ', sep='')
v_cond<-' wrnty_cmnt_typ_cd="SVCAD" '

txt<-paste(' select ', v_obj, ' from ', v_table, ' where ', v_cond, ' group by ', v_group, sep='')
comment_cust<-sql(txt)
createOrReplaceTempView(comment_cust,"comment_cust")

v_table<-paste(adhoc_prefix,"comment_cust",sep="")
create_hive(bucket=800, partition="",
            hivetable=v_table,
            temptable="comment_cust",
            whereclause="",
            clusterclause="cust_clm_nb")


v_obj<-' CURRENT_DATE() as rundate, wrnty_clm_nb as tech_clm_nb, trim(lower(wrnty_cmnt_tx)) as tech_comment '
v_group<-' CURRENT_DATE(), wrnty_clm_nb, trim(lower(wrnty_cmnt_tx))'
v_table<-paste(raw_prefix, 'wrnty_cmnt_crnt_vw ', sep='')
v_cond<-' wrnty_cmnt_typ_cd="TECH" '

txt<-paste(' select ', v_obj, ' from ', v_table, ' where ', v_cond, ' group by ', v_group, sep='')
comment_tech<-sql(txt)
createOrReplaceTempView(comment_tech,"comment_tech")

v_table<-paste(adhoc_prefix,"comment_tech",sep="")
create_hive(bucket=800, partition="",
            hivetable=v_table,
            temptable="comment_tech",
            whereclause="",
            clusterclause="tech_clm_nb")

#########################################
# Step 2: create adhoc_claim_temp
#########################################
v_table <- paste(adhoc_prefix,"vehicle a, ",
                 raw_prefix,"wrnty_clm_crnt_vw b",sep="")

#--- selecting vehicle fields ------
        wrnty_rpr_dt <- " TO_DATE(substr(b.vhcl_rpr_dt,1,10), 'yyyy-MM-dd') as wrnty_rpr_dt "

   wrnty_replace_dt <- " TO_DATE(substr( b.prts_rplc_dt,1,10),'yyyy-MM-dd') as wrnty_replace_dt "

    wrnty_wo_opn_dt <- " TO_DATE(substr( b.clm_wo_opn_dt,1,10),'yyyy-MM-dd') as wrnty_wo_opn_dt "


c_mis_v01<-" CEILING(DATEDIFF(TO_DATE(substr( b.vhcl_rpr_dt,1,10), 'yyyy-MM-dd'),
                              v_orgnl_in_svc_dt)/30.4) "

c_mis_v02<-" CEILING(DATEDIFF(TO_DATE(substr( b.vhcl_rpr_dt,1,10), 'yyyy-MM-dd'),
                              TO_DATE(substr( b.prts_rplc_dt,1,10), 'yyyy-MM-dd'))/30.4) "


std_wrnty<- " CASE WHEN b.cvrg_clas_cd in ('0','3','4','6','8') THEN 'YES' ELSE 'NO' END AS std_wrnty "

sold_condition<-" a.sold_flag='YES' "
replace_condition<- " DATEDIFF(TO_DATE(substr(b.prts_rplc_dt,1,10), 'yyyy-MM-dd'), a.v_mnfctg_dt) >0 "
non_negative_g1=paste(c_mis_v01,">0",sep="")


# 2018.04.30: New logic:  if sold and repair date is before sold date, count as 0 MIS #
c_mis <- paste("( CASE when a.sold_flag='NO' then 0
                      else (case when ", replace_condition,  " then ", c_mis_v02,
                               " when ", non_negative_g1, " then " ,c_mis_v01,
                               " else 0 END)
                  END ) as c_mis ", sep="")

gapdays_v01<-" ROUND(DATEDIFF(TO_DATE(substr( b.vhcl_rpr_dt,1,10), 'yyyy-MM-dd'),
                              v_orgnl_in_svc_dt )) "
gapdays_v02<-" ROUND(DATEDIFF(TO_DATE(substr( b.vhcl_rpr_dt,1,10), 'yyyy-MM-dd'),
                              TO_DATE(substr( b.prts_rplc_dt,1,10), 'yyyy-MM-dd') )) "
gapdays<- paste(" (case when ", replace_condition,
                " then ", gapdays_v02,
                " else case when ", non_negative_g1,
                " then ", gapdays_v01,
                " else 1 END END)",sep="")

gapmiles<- paste(" (case when ", replace_condition,
                " then floor(b.vhcl_mlg_nb) - floor(b.vhcl_lst_rplcd_mlg_nb)
                  else case when ", non_negative_g1,
                " then floor(b.vhcl_mlg_nb)
                  else 1 END END)",sep="")

gm_L= paste(" (CASE WHEN ", gapmiles, "<500
            THEN 1 ELSE FLOOR(", gapmiles, "/500)*500
            END) as GAPMILE_BIN_L ",sep="")

gm_U= paste(" (FLOOR(", gapmiles,"/500)+1)*500 as GAPMILE_BIN_U ",sep="")

gmpd= paste(gapmiles, "/",gapdays," as GAPMPD ", sep="")

c_mis_condition <- paste(" (CASE when a.sold_flag='NO'
                         then 0
                         else
                         case when ", replace_condition,
                         " then ", c_mis_v02,
                         " else ", c_mis_v01,
                         "END END) ",sep="")

c_mis_seq<-0:180
c_mis_vec<-paste(" CASE WHEN (", c_mis_condition, ")=",c_mis_seq, " THEN 1 ELSE 0 END as cmis",c_mis_seq,sep="",collapse=",")

v_fields <- paste(" b.vin_id as vin_id,
        b.wrnty_clm_nb as wrnty_clm_nb,
        substr(b.vhcl_rpr_dt,1,10) as repair_date,
        concat(substr(b.vhcl_rpr_dt,1,4),substr(b.vhcl_rpr_dt,6,2)) as repairmth,
        floor(b.vhcl_mlg_nb) as mileage,
        substr(b.prts_rplc_dt,1,10) as prts_rplc_dt,
        floor(b.vhcl_lst_rplcd_mlg_nb) as lastrepl_mileage,
        b.rcvd_pfp_nb,
        b.svc_dlr_nb as svc_dlr_nb,
        d.lbr_oprtn_cd as PRIMARY_OPCD,
        CASE WHEN isnull(c.cnsdtd_pfp_nb) THEN b.fqi_wrnty_pfp_nb  ELSE c.cnsdtd_pfp_nb END as CONSOLIDATED_PFP5,
        b.fqi_wrnty_pfp_nb as FQI_PFP5,
        b.rcvd_pfp_1st_5_nb as RCVD_PFP5,
        b.drvd_pfp_1st_5_nb as DRVD_PFP5,
        b.wrnty_orgnl_pnc_id as ORGNL_PNC,
        b.drvd_pnc_id as DRVD_PNC,
        b.orgnl_trbl_cd as CT,
        b.orgnl_symptm_cd as CS,
        b.dlr_rpr_ordr_nb,
        b.wo_nb,
        b.wol_nb as wol_itm_nb,
        b.clm_typ_cd,
        b.clm_bsns_typ_cd,
        b.wrnty_clm_blng_cd,
        b.wrnty_clm_sts_cd,
        b.clm_rjct_rsn_cd,
        b.clm_lblty_src_cd,
        b.blng_prcs_in,
        b.rqstd_cvrg_cd,
        b.rpt_exclsn_in,
        b.wrnty_aflt_rpt_exclsn_rfrnc_nb,
        b.clm_dstrbr_cd as wrnty_rpr_dstrbr_cd,
        b.dlr_crncy_cd,
        b.dlr_prt_am,
        b.dlr_lbr_am,
        b.dlr_sblt_am,
        b.dlr_totl_am,
        b.dlr_prt_orgnl_crncy_am,
        b.dlr_lbr_orgnl_crncy_am,
        b.dlr_sblt_orgnl_crncy_am,
        b.dlr_totl_orgnl_crncy_am,
        b.clm_fncl_dsbrmt_dt_nb,
        b.clm_net_am,
        b.clm_rcvry_am,
        b.clm_dsgn_rcvry_am,
        b.clm_dstrbr_prt_am,
        b.clm_dstrbr_lbr_am,
        b.clm_dstrbr_sblt_am,
        b.clm_dstrbr_totl_am,
        b.dstrbr_prt_orgnl_crncy_am,
        b.dstrbr_lbr_orgnl_crncy_am,
        b.dstrbr_sblt_orgnl_crncy_am,
        b.dstrbr_totl_orgnl_crncy_am,
        b.cvrg_clas_cd,
        b.wrnty_btry_dgnstc_cd,
        b.wrnty_clm_trtry_cd,
        b.vhf_qlfr_cd,
        b.svc_aprvl_rqst_in,
        dlr.dlr_nm as svc_dlr_name,
        dlr.dlr_st_cd as svc_state,
        dlr.dlr_pstl_cd as svc_zipcode,
        substr(dlr.dlr_pstl_cd, 1,3) as svc_zip3,
        CASE WHEN isnull(altitude.zip3) THEN 0 ELSE 1 END as c_altitude,
        CASE WHEN isnull(cold.zip3) THEN 0 ELSE 1 END as c_cold,
        CASE WHEN isnull(dry.zip3) THEN 0 ELSE 1 END as c_dry,
        CASE WHEN isnull(hot.zip3) THEN 0 ELSE 1 END as c_hot,
        CASE WHEN isnull(humid.zip3) THEN 0 ELSE 1 END as c_humid,
        CASE WHEN isnull(rain.zip3) THEN 0 ELSE 1 END as c_rain,
        CASE WHEN isnull(salt.zip3) THEN 0 ELSE 1 END as c_salt,
        CASE WHEN isnull(solar.zip3) THEN 0 ELSE 1 END as c_solar,"
        ,wrnty_rpr_dt, ","  ,wrnty_replace_dt, ","  ,wrnty_wo_opn_dt, "," ,c_mis, ","
        ,c_mis_vec,"," ,gapdays," as gapdays," ,gapmiles," as gapmiles,"
        ,gm_L, ","  ,gm_U,  ",",gmpd,sep="")

v_fields=gsub("\n", " ", v_fields)

#--building vehicle filters----
v_condition <- " a.vvin_id= b.vin_id "

v_leftouter<-paste(" LEFT OUTER JOIN ", raw_prefix, "dlr_dm dlr
    ON dlr.dlr_nb =  b.svc_dlr_nb AND
    unix_timestamp(trim(dlr.dlr_dm_end_dt),'MM/dd/yyyy HH:mm:ss.SSSSSSS') >  unix_timestamp(current_timestamp)
    and dlr.aflt_cmpny_cd = b.aflt_cmpny_cd
 LEFT OUTER JOIN equip.fqi_pfp c
    ON b.fqi_wrnty_pfp_nb=  c.fqi_pfp_nb
 LEFT OUTER JOIN ", raw_prefix, "svc_oprtn_crnt_vw d
    ON b.wrnty_clm_nb =  d.wrnty_clm_nb
    AND d.svc_oprtn_prmry_in=TRUE
 LEFT OUTER JOIN equip.altitude_zipcode altitude
    ON substr(dlr.dlr_pstl_cd, 1,3) = altitude.zip3
    AND upper(dlr.dlr_st_cd) = upper(substr(altitude.state,1,2))
 LEFT OUTER JOIN equip.cold_zipcode cold
    ON substr(dlr.dlr_pstl_cd, 1,3) = cold.zip3
    AND upper(dlr.dlr_st_cd) = upper(substr(cold.state,1,2))
 LEFT OUTER JOIN equip.dry_zipcode dry
    ON substr(dlr.dlr_pstl_cd, 1,3)= dry.zip3
    AND upper(dlr.dlr_st_cd)= upper(substr(dry.state,1,2))
 LEFT OUTER JOIN equip.hot_zipcode hot
    ON substr(dlr.dlr_pstl_cd, 1,3) = hot.zip3
    AND upper(dlr.dlr_st_cd) = upper(substr(hot.state,1,2))
 LEFT OUTER JOIN equip.humid_zipcode humid
    ON substr(dlr.dlr_pstl_cd, 1,3) = humid.zip3
    AND upper(dlr.dlr_st_cd)= upper(substr(humid.state,1,2))
 LEFT OUTER JOIN equip.rain_zipcode rain
    ON substr(dlr.dlr_pstl_cd, 1,3) = rain.zip3
    AND upper(dlr.dlr_st_cd)= upper(substr(rain.state,1,2))
 LEFT OUTER JOIN equip.salt_zipcode salt
    ON substr(dlr.dlr_pstl_cd, 1,3) = salt.zip3
    AND upper(dlr.dlr_st_cd)= upper(substr(salt.state,1,2))
 LEFT OUTER JOIN equip.solar_zipcode solar
    ON substr(dlr.dlr_pstl_cd, 1,3)= solar.zip3
    AND upper(dlr.dlr_st_cd) = upper(substr(solar.state,1,2)) ",sep="")


v_leftouter=gsub("\n"," ",v_leftouter)


txt=paste(" SELECT ",
           v_fields," FROM ",v_table, v_leftouter, " WHERE ", v_condition)
txt=gsub("\n", " ",txt)
adhoc_temp_claim=sql(txt)
createOrReplaceTempView(adhoc_temp_claim, "adhoc_temp_claim")

#v_table<-paste(adhoc_prefix,"temp_claim",sep="")
#create_hive(bucket=2400, partition="",
#            hivetable=v_table,
#           temptable="adhoc_temp_claim",
#            whereclause="",
#           clusterclause="wrnty_clm_nb")

########################################
# Step 3: get last claim update
########################################
v_table<-paste(raw_prefix, "wrnty_clm_crnt_vw",sep="")
v_obj<-"1 as TableFlag, max(updt_ts) as claim_lastupdate "
txt<-paste(" SELECT ", v_obj, " from ", v_table, sep="")
adhoc_claim_lastupdate=sql(txt)
createOrReplaceTempView(adhoc_claim_lastupdate, "adhoc_claim_lastupdate")


########################################
# Step 4: Insert GCAR Coverage
########################################
v_table<-paste(adhoc_prefix, "vehicle a, adas ORGNL_PNC,
        b.drvd_pnc_id as DRVD_PNC,
        b.orgnl_trbl_cd as CT,
        b.orgnl_symptm_cd as CS,
        b.dlr_rpr_ordr_nb,
        b.wo_nb,
        b.wol_nb as wol_itm_nb,
        b.clm_typ_cd,
        b.clm_bsns_typ_cd,
        b.wrnty_clm_blng_cd,
        b.wrnty_clm_sts_cd,
        b.clm_rjct_rsn_cd,
        b.clm_lblty_src_cd,
        b.blng_prcs_in,
        b.rqstd_cvrg_cd,
        b.rpt_exclsn_in,
        b.wrnty_aflt_rpt_exclsn_rfrnc_nb,
        b.clm_dstrbr_cd as wrnty_rpr_dstrbr_cd,
        b.dlr_crncy_cd,
        b.dlr_prt_am,
        b.dlr_lbr_am,
        b.dlr_sblt_am,
        b.dlr_totl_am,
        b.dlr_prt_orgnl_crncy_am,
        b.dlr_lbr_orgnl_crncy_am,
        b.dlr_sblt_orgnl_crncy_am,
        b.dlr_totl_orgnl_crncy_am,
        b.clm_fncl_dsbrmt_dt_nb,
        b.clm_net_am,
        b.clm_rcvry_am,
        b.clm_dsgn_rcvry_am,
        b.clm_dstrbr_prt_am,
        b.clm_dstrbr_lbr_am,
        b.clm_dstrbr_sblt_am,
        b.clm_dstrbr_totl_am,
        b.dstrbr_prt_orgnl_crncy_am,
        b.dstrbr_lbr_orgnl_crncy_am,
        b.dstrbr_sblt_orgnl_crncy_am,
        b.dstrbr_totl_orgnl_crncy_am,
        b.cvrg_clas_cd,
        b.wrnty_btry_dgnstc_cd,
        b.wrnty_clm_trtry_cd,
        b.vhf_qlfr_cd,
        b.svc_aprvl_rqst_in,
        dlr.dlr_nm as svc_dlr_name,
        dlr.dlr_st_cd as svc_state,
        dlr.dlr_pstl_cd as svc_zipcode,
        substr(dlr.dlr_pstl_cd, 1,3) as svc_zip3,
        CASE WHEN isnull(altitude.zip3) THEN 0 ELSE 1 END as c_altitude,
        CASE WHEN isnull(cold.zip3) THEN 0 ELSE 1 END as c_cold,
        CASE WHEN isnull(dry.zip3) THEN 0 ELSE 1 END as c_dry,
        CASE WHEN isnull(hot.zip3) THEN 0 ELSE 1 END as c_hot,
        CASE WHEN isnull(humid.zip3) THEN 0 ELSE 1 END as c_humid,
        CASE WHEN isnull(rain.zip3) THEN 0 ELSE 1 END as c_rain,
        CASE WHEN isnull(salt.zip3) THEN 0 ELSE 1 END as c_salt,
        CASE WHEN isnull(solar.zip3) THEN 0 ELSE 1 END as c_solar,"
        ,wrnty_rpr_dt, ","  ,wrnty_replace_dt, ","  ,wrnty_wo_opn_dt, "," ,c_mis, ","
        ,c_mis_vec,"," ,gapdays," as gapdays," ,gapmiles," as gapmiles,"
        ,gm_L, ","  ,gm_U,  ",",gmpd,sep="")

v_fields=gsub("\n", " ", v_fields)

#--building vehicle filters----
v_condition <- " a.vvin_id= b.vin_id "

v_leftouter<-paste(" LEFT OUTER JOIN ", raw_prefix, "dlr_dm dlr
    ON dlr.dlr_nb =  b.svc_dlr_nb AND
    unix_timestamp(trim(dlr.dlr_dm_end_dt),'MM/dd/yyyy HH:mm:ss.SSSSSSS') >  unix_timestamp(current_timestamp)
    and dlr.aflt_cmpny_cd = b.aflt_cmpny_cd
 LEFT OUTER JOIN equip.fqi_pfp c
    ON b.fqi_wrnty_pfp_nb=  c.fqi_pfp_nb
 LEFT OUTER JOIN ", raw_prefix, "svc_oprtn_crnt_vw d
    ON b.wrnty_clm_nb =  d.wrnty_clm_nb
    AND d.svc_oprtn_prmry_in=TRUE
 LEFT OUTER JOIN equip.altitude_zipcode altitude
    ON substr(dlr.dlr_pstl_cd, 1,3) = altitude.zip3
    AND upper(dlr.dlr_st_cd) = upper(substr(altitude.state,1,2))
 LEFT OUTER JOIN equip.cold_zipcode cold
    ON substr(dlr.dlr_pstl_cd, 1,3) = cold.zip3
    AND upper(dlr.dlr_st_cd) = upper(substr(cold.state,1,2))
 LEFT OUTER JOIN equip.dry_zipcode dry
    ON substr(dlr.dlr_pstl_cd, 1,3)= dry.zip3
    AND upper(dlr.dlr_st_cd)= upper(substr(dry.state,1,2))
 LEFT OUTER JOIN equip.hot_zipcode hot
    ON substr(dlr.dlr_pstl_cd, 1,3) = hot.zip3
    AND upper(dlr.dlr_st_cd) = upper(substr(hot.state,1,2))
 LEFT OUTER JOIN equip.humid_zipcode humid
    ON substr(dlr.dlr_pstl_cd, 1,3) = humid.zip3
    AND upper(dlr.dlr_st_cd)= upper(substr(humid.state,1,2))
 LEFT OUTER JOIN equip.rain_zipcode rain
    ON substr(dlr.dlr_pstl_cd, 1,3) = rain.zip3
    AND upper(dlr.dlr_st_cd)= upper(substr(rain.state,1,2))
 LEFT OUTER JOIN equip.salt_zipcode salt
    ON substr(dlr.dlr_pstl_cd, 1,3) = salt.zip3
    AND upper(dlr.dlr_st_cd)= upper(substr(salt.state,1,2))
 LEFT OUTER JOIN equip.solar_zipcode solar
    ON substr(dlr.dlr_pstl_cd, 1,3)= solar.zip3
    AND upper(dlr.dlr_st_cd) = upper(substr(solar.state,1,2)) ",sep="")


v_leftouter=gsub("\n"," ",v_leftouter)


txt=paste(" SELECT ",
           v_fields," FROM ",v_table, v_leftouter, " WHERE ", v_condition)
txt=gsub("\n", " ",txt)
adhoc_temp_claim=sql(txt)
createOrReplaceTempView(adhoc_temp_claim, "adhoc_temp_claim")

#v_table<-paste(adhoc_prefix,"temp_claim",sep="")
#create_hive(bucket=2400, partition="",
#            hivetable=v_table,
#           temptable="adhoc_temp_claim",
#            whereclause="",
#           clusterclause="wrnty_clm_nb")

########################################
# Step 3: get last claim update
########################################
v_table<-paste(raw_prefix, "wrnty_clm_crnt_vw",sep="")
v_obj<-"1 as TableFlag, max(updt_ts) as claim_lastupdate "
txt<-paste(" SELECT ", v_obj, " from ", v_table, sep="")
adhoc_claim_lastupdate=sql(txt)
createOrReplaceTempView(adhoc_claim_lastupdate, "adhoc_claim_lastupdate")


########################################
# Step 4: Insert GCAR Coverage
########################################
v_table<-paste(adhoc_prefix, "vehicle a, ad
