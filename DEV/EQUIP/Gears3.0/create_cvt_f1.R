##########################################################
#  Tracking :  CVT Part use for F1
#  Customer: Nakai
#  Created by: J. Quiton
#  2018.04.09
##########################################################
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
spark_yarn_app_name_ <- 'sparkr-create_tracker_cvtf1'
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

#####################
# Create Table
#####################
region=" ( glbl_mrkt_cd in ('USA','CAN') ) "
classcode= " ( cvrg_clas_cd in (0,8) ) "
myrange= " ( vhcl_yr_nb >= 2013 ) "
cvtpart = " ( part5_nb in ('31020','3102M','310C0','310CM','310CN','31705','3170E') )"

set01 = " ( nml_prdctn_mdl_cd in ('R52','L50','Z52','L33','E52','A36') and vhcl_engn_mdl_cd in ('VQ35') ) "
set02 = " ( nml_prdctn_mdl_cd in ('F15','M20','M30','T32','L33') ) "
set03 = " ( nml_prdctn_mdl_cd in ('N17','E12','B17') ) "

cvt_group=" CASE WHEN  nml_prdctn_mdl_cd in  ('R52','L50','Z52','L33','E52','A36')
                 THEN 'FK*'
                 WHEN  nml_prdctn_mdl_cd in ('F15','M20','M30','T32','L33')
                 THEN 'FKK2'
             ELSE 'DX' END as cvt_group "

issue=paste(" ( ", paste(region,classcode,cvtpart,sep=' and '), " ) ")
models=paste(" ( ", paste(set01,set02,set03,sep= ' or '), " ) ")


##--- output #1: Create Part Use Table ------
v_condition= paste(issue,models, myrange, sep=' and ')
v_table<-paste(adhoc_prefix,"part a ",sep="")
txt=paste(" SELECT ", cvt_group,", a.* from ", v_table, " where ", v_condition,sep="")
cvt_part<-sql(txt)
createOrReplaceTempView(cvt_part,"cvt_part")

v_table<-paste(adhoc_prefix,"tracking_CVTPartUse_F1",sep="")
v_cluster="vhcl_yr_nb, nml_prdctn_mdl_cd, glbl_mrkt_cd , retail_state "
create_hive(bucket=80, partition="cvt_group",
            hivetable=v_table, temptable="cvt_part",
            whereclause="",
            clusterclause=v_cluster)

#--- output #2:  Create Claim table ----------#
v_table<-paste(adhoc_prefix,"tracking_CVTPartUse_F1",sep="")
txt=paste(" SELECT DISTINCT  wrnty_clm_nb, cvt_group from ", v_table, sep="")
cvt_claimnb=sql(txt)
createOrReplaceTempView(cvt_claimnb,"cvt_claimnb")

v_obj= " CURRENT_DATE() as Tracking_ReportDate, a.cvt_group, b.* "
v_table= paste(" cvt_claimnb a, ", adhoc_prefix, "claim b ",sep="")
v_condition = " a.wrnty_clm_nb = b.wrnty_clm_nb "
txt=paste(' SELECT ', v_obj, ' from  ', v_table, ' where ' , v_condition)
cvt_final<-sql(txt)
createOrReplaceTempView(cvt_final,"cvt_final")

v_table<-paste(adhoc_prefix,"tracking_CVTClaims_F1",sep="")
v_cluster="vhcl_yr_nb, nml_prdctn_mdl_cd, glbl_mrkt_cd , retail_state "
create_hive(bucket=80, partition="cvt_group",
            hivetable=v_table, temptable="cvt_final",
            whereclause="",
            clusterclause=v_cluster)
