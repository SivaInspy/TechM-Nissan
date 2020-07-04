#####################################################################
#  Creating Adhoc PART
#  Created by: Quiton, J
#  Update date: 2019.04.12
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
spark_yarn_app_name_ <- 'sparkr-create_adhoc_part'
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

######################################################
#  create polished "final" part table
######################################################
v_table <- paste(adhoc_prefix, "claim_dm a, ",
                 raw_prefix, "svc_prt_crnt_vw b ",sep="")
v_fields <-"a.*,
            TO_DATE(substr(b.crte_ts,1,10),'yyyy-MM-dd') as transaction_dt,
            TO_DATE(substr(b.updt_ts,1,10),'yyyy-MM-dd') as update_dt,
            b.prt_nb as part_nb,
            b.sts_cd as part_status,
            substr(b.prt_nb,1,5) as part5_nb,
            c.pfp_ds as part5_desc,
            concat(b.prt_nb, ' - ', c.pfp_ds) as PartID,
            b.vhcl_symptm_cd as part_cscode,
            b.vhcl_trbl_cd as part_ctcode,
            b.pfp_in as part_primary_in,
            b.prt_cst_am as part_cost,
            b.prt_qt as part_qty,
            b.prt_cst_orgnl_crncy_am as part_costcurrency"

v_leftouter<-paste(" LEFT OUTER JOIN ", raw_prefix, "pfp_dm c
                        ON substr(b.prt_nb,1,5) = c.pfp_nb",sep="")

v_leftouter<-gsub("\n", " ",v_leftouter)


#--building vehicle filters----
#  2018.04.09:  Removed filter b.sts_cd = 'AP'"

v_condition <- "a.vvin_id= b. vin_id
                and a.wrnty_clm_nb=b.wrnty_clm_nb"


#---building the temporary table ------------
txt=paste("SELECT DISTINCT ",
           v_fields," FROM ",v_table, v_leftouter," WHERE ", v_condition)
txt=gsub("\n", " ",txt)
fpart=sql(txt)
createOrReplaceTempView(fpart,"fpart")
cacheTable("fpart")

#####################
#  ADHOC_PART
#####################
v_table<-paste(adhoc_prefix,"part",sep="")
v_cluster<-"a.part_nb, a.cvrg_clas_cd,
            a.nml_prdctn_mdl_cd,  a.glbl_mrkt_cd, a.mnfctg_vhcl_plnt_cd,
            a.nna_sls_mdl_cd, a.vhcl_engn_mdl_cd,
            a.v_mnfctg_dt, a.v_orgnl_in_svc_dt, a.vvin_id,  a.wrnty_clm_nb "
create_hive(bucket=bucket.xlarge, partition="vhcl_yr_nb",
            hivetable=v_table, temptable="fpart",
            whereclause="",
            clusterclause=v_cluster)


######################
#  ADHOC_VEHICLEPART
######################
v_obj=" b.update_dt as part_update_dt, b.repair_date, b.PartID, b.part_ctcode, b.part_cscode, b.rcvd_pfp5,
        b.part_qty, a.nml_prdctn_mdl_cd, b.svc_dlr_nb, b.svc_state,
        b.wrnty_clm_nb, b.wo_nb, b.wol_itm_nb,  b.drvd_pnc, a.nna_sls_mdl_cd,
        b.part_costcurrency, a.extr_clr_cd,
        a.glbl_mrkt_cd, b.part5_nb, a.v_mnfctg_dt, b.orgnl_pnc, a.orgnl_in_svc_dt,
        b.part_cost, a.vvin_id, b.part5_desc, a.prodmth, b.repairmth, b.part_primary_in,
        a.vhcl_yr_nb, a.vhcl_engn_mdl_cd, b.primary_opcd, b.drvd_pfp5,
        b.consolidated_pfp5, a.v_orgnl_in_svc_dt, b.part_nb, a.vhcl_flt_in,
        a.mnfctg_vhcl_plnt_cd, b.cvrg_clas_cd, round(b.mileage) as mileage"

v_table<-paste(adhoc_prefix,"vehicle a ",sep="")
v_leftouter<-" left outer join fpart b
                    on a.vvin_id = b.vvin_id "
txt= paste(" SELECT distinct ", v_obj,    " from ", v_table, v_leftouter,sep="")

vehiclepart<-sql(txt)
createOrReplaceTempView(vehiclepart,"vehiclepart")

v_cluster<-" a.part_nb, a.cvrg_clas_cd, a.wrnty_clm_nb,
             a.nml_prdctn_mdl_cd, a.glbl_mrkt_cd, a.mnfctg_vhcl_plnt_cd, a.nna_sls_mdl_cd, a.vhcl_engn_mdl_cd,
             a.v_mnfctg_dt, a.v_orgnl_in_svc_dt, a.vvin_id  "
v_table<-paste(adhoc_prefix,"vehiclepart",sep="")
v_cluster<-"a.part_nb, a.cvrg_clas_cd,
            a.nml_prdctn_mdl_cd,  a.glbl_mrkt_cd, a.mnfctg_vhcl_plnt_cd,
            a.nna_sls_mdl_cd, a.vhcl_engn_mdl_cd,
            a.v_mnfctg_dt, a.v_orgnl_in_svc_dt, a.vvin_id,  a.wrnty_clm_nb "
create_hive(bucket=bucket.xlarge, partition="vhcl_yr_nb",
            hivetable=v_table, temptable="vehiclepart",
            whereclause="",
            clusterclause=v_cluster)

quit(save="yes")
