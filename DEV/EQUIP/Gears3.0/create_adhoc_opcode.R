#####################################################################
#  Creating Adhoc Opcode
#  Created by: Quiton, J
#  Update date: 2019.02.20
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
 bucket_vec<-c(1,2,4,10,20,bseq)

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
spark_yarn_app_name_ <- 'sparkr-create_adhoc_opcode'
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

################################
# Create opcode table
#################################
v_table <- paste(adhoc_prefix,"claim_dm a, ",
                 raw_prefix,"svc_oprtn_crnt_vw b",sep="")

v_fields <-"a.*,
        TO_DATE(substr(b.crte_ts,1,10),'yyyy-MM-dd') as transaction_dt,
        TO_DATE(substr(b.updt_ts,1,10),'yyyy-MM-dd') as update_dt,
        b.lbr_oprtn_cd as Lbr_OpCode,
        substr(b.lbr_oprtn_cd,1,5) as OpCode5,
        c.pfp_ds as OpCode5_desc,
        concat(b.lbr_oprtn_cd, ' - ', c.pfp_ds) as OpCodeID,
        b.vhcl_symptm_cd as OpCode_cscode,
        b.vhcl_trbl_cd as OpCode_ctcode,
        b.svc_oprtn_prmry_in as OpCode_primary_in,
        b.lbr_hr_nb as OpCode_hours "

v_leftouter<-paste(" LEFT OUTER JOIN ", raw_prefix, "pfp_dm c
                           ON substr(b.lbr_oprtn_cd,1,5) = c.pfp_nb",sep="")

v_leftouter<-gsub("\n", " ",v_leftouter)


#--building vehicle filters----
v_condition <- "a.vvin_id= b.vin_id
               and a.wrnty_clm_nb=b.wrnty_clm_nb
               and b.sts_cd ='AP'"

#---building the temporary table ------------
txt=paste('SELECT DISTINCT ',
           v_fields,' FROM ',v_table, v_leftouter,' WHERE ', v_condition)
txt=gsub('\n', ' ',txt)
labor=sql(txt)
createOrReplaceTempView(labor,"labor")
cacheTable("labor")

#####################
#  ADHOC_OPCODE
#####################
v_cluster<-"a.Lbr_OpCode, a.cvrg_clas_cd,
            a.nml_prdctn_mdl_cd, a.glbl_mrkt_cd, a.mnfctg_vhcl_plnt_cd,
            a.nna_sls_mdl_cd, a.vhcl_engn_mdl_cd,
            a.v_mnfctg_dt, a.v_orgnl_in_svc_dt, a.vvin_id, a.wrnty_clm_nb"

v_table<-paste(adhoc_prefix,"OpCode",sep="")
create_hive(bucket=bucket.xlarge, partition="vhcl_yr_nb",
            hivetable=v_table, temptable="labor",
            whereclause="",
            clusterclause=v_cluster)

######################
#  ADHOC_VEHICLEOPCODE
######################
v_obj=" b.update_dt as opcode_update_dt, b.repair_date, b.OpCodeID, b.OpCode_ctcode, b.OpCode_cscode, b.rcvd_pfp5, b.OpCode_hours, a.nml_prdctn_mdl_cd, b.svc_dlr_nb, b.svc_state,
        b.wrnty_clm_nb, b.wo_nb, b.wol_itm_nb,  b.drvd_pnc, a.nna_sls_mdl_cd, a.extr_clr_cd,
        a.glbl_mrkt_cd, b.OpCode5, a.v_mnfctg_dt, b.orgnl_pnc, a.orgnl_in_svc_dt,
        a.vvin_id, b.OpCode5_desc, a.prodmth, b.repairmth, b.OpCode_primary_in,
        a.vhcl_yr_nb, a.vhcl_engn_mdl_cd, b.primary_opcd, b.drvd_pfp5,
        b.consolidated_pfp5, a.v_orgnl_in_svc_dt, b.Lbr_OpCode, a.vhcl_flt_in, a.mnfctg_vhcl_plnt_cd, b.cvrg_clas_cd, round(b.mileage,0) as mileage"

v_table<-paste(adhoc_prefix,"vehicle a",sep="")
v_leftouter<-" left outer join labor b
                  on a.vvin_id = b.vvin_id "
txt= paste(" SELECT distinct ", v_obj, " from  ", v_table, v_leftouter,sep="")
vehicleopcode<-sql(txt)
createOrReplaceTempView(vehicleopcode,"vehicleopcode")

v_table<-paste(adhoc_prefix,"vehicleopcode",sep="")
v_cluster<-" a.Lbr_OpCode, a.cvrg_clas_cd,
             a.nml_prdctn_mdl_cd, a.glbl_mrkt_cd, a.mnfctg_vhcl_plnt_cd,
             a.nna_sls_mdl_cd, a.vhcl_engn_mdl_cd,
             a.v_mnfctg_dt, a.v_orgnl_in_svc_dt, a.vvin_id,  a.wrnty_clm_nb "
create_hive(bucket=bucket.xlarge, partition="vhcl_yr_nb",
            hivetable=v_table, temptable="vehicleopcode",
            whereclause="",
            clusterclause=v_cluster)

rm(list=ls())
quit()
