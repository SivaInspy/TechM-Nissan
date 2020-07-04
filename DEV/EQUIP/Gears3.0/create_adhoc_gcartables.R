#####################################################################
#  Creating Adhoc optimized Gcar tables
#  Created by: Quiton, J
#  for NNA_TCS_COE
#
#  Update date: 2019.10.14
#####################################################################
    bucket.tiny<-20
   bucket.small<-40
  bucket.medium<-80
   bucket.large<-160
  bucket.xlarge<-320
 bucket.xxlarge<-640

##########################
# GLOBAL PARAMETERS
###########################
CREATE_TABLE_FLAG=TRUE
db_name<-"EQUIP"
db_drive_source<-"DRIVE"
drive_project<-"prjct_base"
drive_plant<-"plnt_cd_aray_base"
drive_model<-"mdl_cd_aray_base"
drive_modelyear<-"mdl_yr_aray_base"
drive_req<-"rqst_base"
drive_vb<-"vhcl_base"
drive_adptn<-"adptn_data_base"
drive_adr<-"adr_base"
drive_cmdta<-"cmdta_base"
drive_workflow<-"wrkflw_base"

raw_prefix<-paste(db_name,".adhoc_raw_",sep="")
adhoc_prefix<-paste(db_name,".adhoc_",sep="")
spark_yarn_app_name_ <- paste('sparkr-create_gcar_tables',Sys.time())
loglevel <- 'ERROR'

########################
# Start Spark session
#########################
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


##########################################
# Table 0: Optimized Project x Model
##########################################
v_table<-paste(db_drive_source,".", drive_model,sep="")
v_obj<-" mdl_cd_aray_ky as prjct_ky, parnt_tbl_typ_tx,
         trim(regexp_replace(mdl_cd_elmnt_tx,'\"','')) as mdl_cd_elmnt_tx "
v_group<-" mdl_cd_aray_ky, parnt_tbl_typ_tx, trim(regexp_replace(mdl_cd_elmnt_tx,'\"',''))"
v_cond<-"  parnt_tbl_typ_tx in ('PRJCT') "
txt<-paste("select ", v_obj," from ",v_table, " where ", v_cond, " group by ", v_group,sep="")
pro_mdl_cd<-sql(txt)
createOrReplaceTempView(pro_mdl_cd, "pro_mdl_cd")

##########################################
# Table 0: Optimized Project x ModelYear
##########################################
v_table<-paste(db_drive_source,".", drive_modelyear,sep="")
v_obj<-" mdl_yr_aray_ky as prjct_ky, parnt_tbl_typ_tx,
         trim(regexp_replace(mdl_yr_elmnt_tx,'\"','')) as mdl_yr_elmnt_tx "
v_group<-" mdl_yr_aray_ky, parnt_tbl_typ_tx, regexp_replace(mdl_yr_elmnt_tx,'\"','')"
v_cond<-"  parnt_tbl_typ_tx in ('PRJCT') "
txt<-paste("select ", v_obj," from ",v_table, " where ", v_cond, " group by ", v_group,sep="")
pro_my<-sql(txt)
createOrReplaceTempView(pro_my, "pro_my")

v_table<-paste(adhoc_prefix,"gcar_projectxmy", sep="")
create_hive(bucket=bucket.tiny,
            partition="",
            hivetable=v_table,
            temptable=" pro_my ",
            whereclause="",
            clusterclause=" prjct_ky " )

##########################################
# Table 0: Optimized Project x Plant
##########################################
v_table<-paste(db_drive_source,".",drive_plant,sep="")
v_obj<-" plnt_cd_aray_ky as prjct_ky, parnt_tbl_typ_tx,
         trim(regexp_replace(plnt_cd_aray_elmnt_tx,'\"','')) as plnt_cd_aray_elmnt_tx "
v_group<-" plnt_cd_aray_ky, parnt_tbl_typ_tx, regexp_replace(plnt_cd_aray_elmnt_tx,'\"','')"
v_cond<-"  parnt_tbl_typ_tx in ('PRJCT') "
txt<-paste("select ", v_obj," from ",v_table, " where ", v_cond, " group by ", v_group,sep="")
pro_plant<-sql(txt)
createOrReplaceTempView(pro_plant, "pro_plant")

v_table<-paste(adhoc_prefix,"gcar_projectxplant", sep="")
create_hive(bucket=bucket.tiny,
            partition="",
            hivetable=v_table,
            temptable=" pro_plant ",
            whereclause="",
            clusterclause=" prjct_ky " )

##########################################
# Table 1:  Optimized Project
##########################################
#--- adding workflow table to project
v_obj<-"b.dcmnt_id, b.dcmnt_typ_tx,
        b.athr_usr_id,      b.athr_dprtmt_cd,           b.athr_role_cd,        b.athr_cmpny_cd,
        b.asgnd_by_usr_id,  b.asgnd_by_dprtmt_cd,       b.asgnd_by_role_cd,    b.asgnd_by_cmpny_cd,
        b.lst_mdfyd_usr_id, b.lst_mdfyd_dprtmt_cd,      b.lst_mdfyd_role_cd,   b.lst_mdfyd_cmpny_cd,
        to_date(substr(b.pblshd_ts,1,10), 'yyyy-mm-dd') as pblshd_ts,
        to_date(substr(b.asgnd_by_ts, 1,10), 'yyyy-mm-dd') as asgnd_by_ts,
        b.sts_cd "
v_group<-"b.dcmnt_id, b.dcmnt_typ_tx,
        b.athr_usr_id,      b.athr_dprtmt_cd,           b.athr_role_cd,        b.athr_cmpny_cd,
        b.asgnd_by_usr_id,  b.asgnd_by_dprtmt_cd,       b.asgnd_by_role_cd,    b.asgnd_by_cmpny_cd,
        b.lst_mdfyd_usr_id, b.lst_mdfyd_dprtmt_cd,      b.lst_mdfyd_role_cd,   b.lst_mdfyd_cmpny_cd,
        to_date(substr(b.pblshd_ts,1,10), 'yyyy-mm-dd'),
        to_date(substr(b.asgnd_by_ts, 1,10), 'yyyy-mm-dd'),
        b.sts_cd "

v_table<-paste(db_drive_source, ".", drive_workflow," b ",sep="")
v_cond<-"b.dcmnt_typ_tx='PRO'"
txt<-paste("select ", v_obj, " from ", v_table, " where ", v_cond, " group by ", v_group ,sep="")
workflow_pro<-sql(txt)

createOrReplaceTempView(workflow_pro,"workflow_pro")

#--- flattening modelcode
v_obj<-"prjct_ky, concat_ws(';',collect_set(mdl_cd_elmnt_tx)) as modelcodetx"
v_group<-"prjct_ky"
v_table<-"pro_mdl_cd"
txt<-paste("select ",v_obj, " from ", v_table, " group by ", v_group)
pro_model<-sql(txt)
createOrReplaceTempView(pro_model,"pro_model")

#--- flattening modelyear
v_obj<-"prjct_ky, concat_ws(';',collect_set(mdl_yr_elmnt_tx)) as  mdl_yr_tx"
v_group<-"prjct_ky"
v_table<-paste(adhoc_prefix,"gcar_projectxmy",sep="")
txt<-paste("select ",v_obj, " from ", v_table, " group by ", v_group)
pro_modelyear<-sql(txt)
createOrReplaceTempView(pro_modelyear,"pro_modelyear")

#--- flattening plant
v_obj<-"prjct_ky, concat_ws(';',collect_set(plnt_cd_aray_elmnt_tx)) as  plnt_cd_tx"
v_group<-"prjct_ky"
v_table<-paste(adhoc_prefix,"gcar_projectxplant",sep="")
txt<-paste("select ",v_obj, " from ", v_table, " group by ", v_group)
pro_plantcd<-sql(txt)
createOrReplaceTempView(pro_plantcd,"pro_plantcd")

#--- joining to project----
v_obj<-"a.*, c.modelcodetx, d.mdl_yr_tx, e.plnt_cd_tx, b.*"
v_table<-paste(db_drive_source,".",drive_project," a ",sep="")
v_leftjoin<-" left outer join workflow_pro b on
              a.prjct_ky =b.dcmnt_id
              left outer join pro_model c on
              a.prjct_ky= c.prjct_ky
              left outer join pro_modelyear d on
              a.prjct_ky= d.prjct_ky
              left outer join pro_plantcd e on
              a.prjct_ky= e.prjct_ky "
txt<-paste("select ", v_obj, " from ", v_table, v_leftjoin,sep="")
proj <-sql(txt)

toread<-c('gcars_fn     drive_fn
mproject_uniqueid       mstr_prjct_ky
project_ref_number      rfrnc_nb
subject                 sbjct_tx
priority                prity_tx
taskforcereport         tsk_frc_rpt_tx
priorityjustification   prity_jstfcn_tx
localrefnumber          lcl_rfrnc_nb
enginetype              engn_typ_tx
datefirstincident       intl_incdnt_ts
customercomplaint       cstmr_cmplnt_tx
detailsofconcern        dtl_cncrn_tx
analysisconditions      incdnt_cndtn_tx
startc_warr_date        clm_ts
codunit_code            unt_cd
codpfp                  pfp_tx
codpfp_add              adtnl_pfp_aray_in
codcs_code              symptm_cd
codct_code              trbl_cd
codwarrgroupcode        wrnty_grp_cd
codphenomenon           phnmn_tx
codsm_cat                sbjct_matr_ctgry_tx
estimatedincidentratio   estmtd_incdnt_rato_tx
nrbuybackcases          bybck_case_nb
nrofclaims              clm_qt_tx
totalwarrantycost       wrnty_cst_sld_qt_tx
authorname              athr_usr_id
authororg               athr_cmpny_cd
status                  sts_cd
lastupdatedate          asgnd_by_ts
responsible             asgnd_by_usr_id
pubdate                 pblshd_ts
')

fn_list <- read.table(textConnection(toread), header = TRUE,stringsAsFactors=FALSE)
fn_n<-length(fn_list[,1])

for (idx in 1:fn_n){
proj<-withColumnRenamed(proj, fn_list$drive_fn[idx], fn_list$gcars_fn[idx])
}

createOrReplaceTempView(proj,"proj")
v_table<-paste(adhoc_prefix,"gcar_project", sep="")
create_hive(bucket=bucket.tiny,
            partition="",
            hivetable=v_table,
            temptable=" proj ",
            whereclause=" substr(project_ref_number,1,2) in ('NA') ",
            clusterclause=" project_ref_number " )

##########################################
# Table 2:  Optimized Request
##########################################

#--- adding workflow table to request
v_obj<-"b.dcmnt_id, b.dcmnt_typ_tx,
        b.athr_usr_id,      b.athr_dprtmt_cd,           b.athr_role_cd,        b.athr_cmpny_cd,
        b.asgnd_by_usr_id,  b.asgnd_by_dprtmt_cd,       b.asgnd_by_role_cd,    b.asgnd_by_cmpny_cd,
        b.lst_mdfyd_usr_id, b.lst_mdfyd_dprtmt_cd,      b.lst_mdfyd_role_cd,   b.lst_mdfyd_cmpny_cd,
        to_date(substr(b.pblshd_ts,1,10), 'yyyy-mm-dd') as pblshd_ts,
        to_date(substr(b.asgnd_by_ts, 1,10), 'yyyy-mm-dd') as asgnd_by_ts,
        b.sts_cd "
v_group<-"b.dcmnt_id, b.dcmnt_typ_tx,
        b.athr_usr_id,      b.athr_dprtmt_cd,           b.athr_role_cd,        b.athr_cmpny_cd,
        b.asgnd_by_usr_id,  b.asgnd_by_dprtmt_cd,       b.asgnd_by_role_cd,    b.asgnd_by_cmpny_cd,
        b.lst_mdfyd_usr_id, b.lst_mdfyd_dprtmt_cd,      b.lst_mdfyd_role_cd,   b.lst_mdfyd_cmpny_cd,
        to_date(substr(b.pblshd_ts,1,10), 'yyyy-mm-dd'),
        to_date(substr(b.asgnd_by_ts, 1,10), 'yyyy-mm-dd'),
        b.sts_cd "

v_table<-paste(db_drive_source, ".", drive_workflow," b ",sep="")
v_cond<-"b.dcmnt_typ_tx='REQ'"
txt<-paste("select ", v_obj, " from ", v_table, " where ", v_cond, " group by ", v_group ,sep="")
workflow_req<-sql(txt)
createOrReplaceTempView(workflow_req,"workflow_req")

v_obj<-paste("p.project_ref_number, a.*, b.*",sep="")
v_table<-paste(db_drive_source,".",drive_req, " a, proj p ",sep="")
v_leftjoin<-" left outer join workflow_req b on
              a.rqst_ky =b.dcmnt_id "
v_cond<-" a.prjct_ky=p.prjct_ky "

txt<-paste("select ", v_obj, " from ", v_table, v_leftjoin," where ", v_cond, sep="")
req<-sql(txt)

toread<-c('gcars_fn     drive_fn
cs_code               SYMPTM_CD
ct_code               TRBL_CD
codpfp                PFP_TX
codsm_cat             SBJCT_MATR_CTGRY_TX
codunit_code          UNT_CD
codwarr_group_code    WRNTY_GRP_CD
request_type          RQST_TYP_CD
subject               SBJCT_TX
priority              PRITY_TX
task_force_report     TSK_FRC_RPT_DS
root_cause            ROOT_CAUS_TX
concern_details       CNCRN_DTL_TX
conclusion            CNCLSN_TX
incident_duplication_results   INCDNT_DPLCT_RSLT_TX
investigation_results   IVSTGN_RSLT_TX
pi_request_details      PRDT_IVSTGN_RQST_TX
recurrence_prevention   RLVNT_DSGNTN_RCRNG_PRVNTN_TX
parent_uniqueid         ASCT_RQST_KY
request_ref_number      RFRNC_NB
resmarkettx             MRKT_CD
wfresppersontx          MNZKR_TCSR_PRSN_ROLE_CD
resp_company            RSPSBL_CMPNY_TX
resp_dept               RSPSBL_DPRTMT_TX
resp_org                MNZKR_TCSR_PRSN_CMPNY_CD
currentuser             MNZKR_TCSR_PRSN_USR_ID
classification          CLSFTN_TX
firstaidcountermeasure  CNTRMSR_1ST_AID_TX
temporarycountermeasure TMPRY_CNTRMSR_TX
permanentcountermeasure PRMNT_CNTRMSR_TX
supinv1tx               SPLR_1_RQST_IVSTGN_TX
supcod1tx               SPLR_1_SPLR_TX
suprec1tx               SPLR_1_RCVD_AT_SPLR_TX
authorname              athr_usr_id
authororg               athr_cmpny_cd
status                  sts_cd
lastupdatedate          asgnd_by_ts
responsible             asgnd_by_usr_id
pubdate                 pblshd_ts
')

fn_list <- read.table(textConnection(toread), header = TRUE,stringsAsFactors=FALSE)
fn_list$drive_fn<-tolower(fn_list$drive_fn)
fn_n<-length(fn_list[,1])

for (idx in 1:fn_n){
  req<-withColumnRenamed(req, fn_list$drive_fn[idx], fn_list$gcars_fn[idx])
}

createOrReplaceTempView(req,"req")
v_table<-paste(adhoc_prefix,"gcar_req", sep="")
create_hive(bucket=bucket.tiny,
            partition="",
            hivetable=v_table,
            temptable=" req ",
            whereclause=" substr(request_ref_number,1,2) in ('NA') ",
            clusterclause=" request_ref_number " )

##########################################
# Table 3:  Optimized cd
##########################################

#-- combine adptn_data_base, vb, cmdta_raw
v_obj<-" a.*, b.project_ref_number,  b.request_ref_number, b.subject"
v_table <-paste( db_drive_source, ".", drive_cmdta," a ", sep="")
v_left_join<- " left join req b on a.rqst_ky=b.rqst_ky "
txt<-paste("select ", v_obj, " from ", v_table, v_left_join, sep="")
cd<-sql(txt)

toread<-c("gcars_fn     drive_fn
horizontaldeploymenttx         hrzntl_dplymt_tx
horizontaldeploymentjptx       hrzntl_dplymt_lcl_tx
counter_measure_details        cntrmsr_dtl_lcl_tx
root_cause_reference           root_caus_rfrnc_lcl_tx
cm_type                        cntrmsr_typ_cd
comments                       cmnt_cd
recurrence_prevention          rcrng_prvntn_tx
verification_of_cm             vrfctn_cntrmsr_tx
codcs_code                     symptm_lcl_cd
codct_code                     trbl_cd
codpfp_add                     adtnl_pfp_aray_in
codpfp                         pfp_tx
codphenomenon                  phnmn_tx
codsm_cat                      sbjct_matr_ctgry_tx
codunit_code                   unt_cd
codwarr_group_code             wrnty_grp_cd
dtc_code                       dtc_cd
manufacturing_method           mnfctg_mthd_lcl_tx
load                           load_tx
materials                      mtrl_tx
")

fn_list <- read.table(textConnection(toread), header = TRUE,stringsAsFactors=FALSE)
fn_list$drive_fn<-tolower(fn_list$drive_fn)
fn_n<-length(fn_list[,1])

for (idx in 1:fn_n){
  cd<-withColumnRenamed(cd, fn_list$drive_fn[idx], fn_list$gcars_fn[idx])
}

createOrReplaceTempView(cd,"cd")
create_hive(bucket=bucket.tiny,
            partition="cm_type",
            hivetable=paste(adhoc_prefix,"gcar_cd", sep=""),
            temptable=" cd ",
            whereclause=" substr(request_ref_number,1,2) in ('NA') ",
            clusterclause=" project_ref_number, request_ref_number " )

##########################################
# Table 4:  Optimized ad2
##########################################

#-- max adptn_ts by adptn_data_ky, vhcl_ky
v_group<- "adptn_data_ky, vhcl_ky, cntrmsr_typ_cd, mdl_cd"
v_having<- " adptn_ts is not null"
txt<-paste("select ", v_group, " , max(to_date(substr(adptn_ts,1,10),'yyyy-mm-dd')) as adptn_ts from ", db_drive_source, ".", drive_vb,
           " group by ", v_group, " having ", v_having,sep="")
vb<-sql(txt)
createOrReplaceTempView(vb,"vb")

#-- combine adptn_data_base, vb, cmdta_raw
v_obj<-" a.adptn_data_ky, a.vhcl_ky, a.cntrmsr_typ_cd as cmtypetx, a.mdl_cd as vinmodeltx, a.adptn_ts as adoptiondatedt,
         b.plnt_tx as productionsitetx, c.adr_ky, c.cmdta_ky, d.rqst_ky, d.project_ref_number as projectrefnumber,
         d.request_ref_number as requestrefnumber, e.subject"
v_table <-" vb a "
v_left_join<- paste(" left join ", db_drive_source, ".", drive_adptn, " b on a.adptn_data_ky = b.adptn_data_ky",
                    " left join ", db_drive_source, ".", drive_adr, " c on b.adr_ky = c.adr_ky",
                    " left join  cd d on c.cmdta_ky = d.cmdta_ky
                      left join req e on d.rqst_ky=e.rqst_ky ", sep="")

v_cond <-" where c.adr_ky is not null and c.cmdta_ky is not null and d.rqst_ky is not null
           and substr(d.project_ref_number, 1,2)='NA' "

txt<-paste("select", v_obj, " from ", v_table, v_left_join, v_cond, sep="")

ad_master<-sql(txt)

createOrReplaceTempView(ad_master,"ad_master")

create_hive(bucket=10,
            partition="cmtypetx",
            hivetable=paste(adhoc_prefix,"gcar_ad2", sep=""),
            temptable=" ad_master ",
            whereclause="",
            clusterclause=" projectrefnumber, requestrefnumber, cmdta_ky, adr_ky, adptn_data_ky, vhcl_ky" )

#################################################################################
# Table 5:  Optimized ad_date : max ad date per cdref
#################################################################################
tstamp<-Sys.time()
txt = paste(' select cmdta_ky, cmdta_ky as cdrefnumber,  max(TO_DATE(substr(adoptiondatedt,1,10),"yyyy-mm-dd")) as ad_date
        from ', adhoc_prefix,'gcar_ad2
        group by cmdta_ky ',sep='')

gcar_ad_date<-sql(txt)

createOrReplaceTempView(gcar_ad_date,"gcar_ad_date")
v_table<-paste(adhoc_prefix,"gcar_ad_date", sep="")

create_hive(bucket=10,
            partition="",
            hivetable=v_table,
            temptable=" gcar_ad_date ",
            whereclause="",
            clusterclause=" cdrefnumber, ad_date " )
