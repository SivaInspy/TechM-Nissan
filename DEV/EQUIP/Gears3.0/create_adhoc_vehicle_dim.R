#####################################################################
#  Creating Adhoc Vehicle Dimension
#  for NNA_TCS_COE
#  Created by: Quiton, J
#  Modified by: Quiton, J
#  Update date: 2019.10.04
#####################################################################
 #  HIVE tuning parameter options
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
db_name<-'EQUIP'
raw_prefix<-paste(db_name,'.adhoc_raw_',sep='')
adhoc_prefix<-paste(db_name,'.adhoc_',sep='')


#####################################
# Task 01: Define functions
#####################################

insert_overwrite<-function(partition, hivetable, temptable){
   sql(paste('set spark.sql.shuffle.partitions=', partition, sep=''))
   v_obj<-' a.* '
   txt<-paste(" INSERT OVERWRITE TABLE ", hivetable, " PARTITION (username='",username,"')
                SELECT 1 as TableID, '",run_time,"' as runtime, ",v_obj," from ", temptable, " a ", sep="")
   txt=gsub('\n', ' ',txt)
   txt<-gsub('\\s+', ' ',txt)
   sql(txt)
}

create_hive<-function(partition, hivetable, temptable){
   txt<-paste("DROP TABLE IF EXISTS ", hivetable,sep="")
   sql(txt)
   v_obj=" a.* "

   txt<-paste(" CREATE TABLE ", hivetable, " USING ORC PARTITIONED BY (", partition,
                ") as SELECT 1 as TableID, "
                ,v_obj, " from  ", temptable, " a ", sep="")
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
spark_yarn_app_name_ <- 'sparkr-create_coe_adhoc_vehicle_dm'
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



v_table <- paste(adhoc_prefix,'vehicle a ',sep='')

v_fields<-"TableFlag,  vvin_id,  nml_prdctn_mdl_cd,
           glbl_mrkt_cd,  vhcl_yr_nb,
           mnfctg_vhcl_plnt_cd,  vhcl_engn_mdl_cd, extr_clr_cd,
           vhcl_flt_in, mnfctg_dt,orgnl_in_svc_dt,nna_sls_mdl_cd"

txt<-paste("set spark.sql.shuffle.partitions=", bucket.small,sep="")
collect(sql(txt))


idx='adhoc_vehicle_dm' %in% db_list

if (sum(idx)==0) {
    txt=paste('DROP TABLE IF EXISTS ', adhoc_prefix,'vehicle_dm',sep='')
    sql(txt)
    txt=paste('CREATE TABLE ', adhoc_prefix,'vehicle_dm USING orc PARTITIONED BY (vhcl_yr_nb)
           AS SELECT ',
           v_fields,' FROM ',v_table,
          ' cluster by a.nml_prdctn_mdl_cd, a.glbl_mrkt_cd,  a.mnfctg_vhcl_plnt_cd, a.nna_sls_mdl_cd, a.vhcl_engn_mdl_cd, a.mnfctg_dt, a.orgnl_in_svc_dt, a.vvin_id',
          sep='')
     txt=gsub('\n', ' ',txt)
     sql(txt)
}

sql(paste('use ',db_name))
txt<-paste('select * from ', adhoc_prefix, 'vehicle_dm limit 1',sep='')
v_obj<-names(collect(sql(txt)))
v_obj<-paste(v_obj,collapse="`,`")
v_obj<-paste('`',v_obj,'`',sep='')


txt=paste('INSERT OVERWRITE TABLE ', adhoc_prefix,'vehicle_dm PARTITION (vhcl_yr_nb)
           SELECT ', v_obj,' FROM ',v_table,
          ' cluster by a.nml_prdctn_mdl_cd, a.glbl_mrkt_cd,  a.mnfctg_vhcl_plnt_cd, a.nna_sls_mdl_cd, a.vhcl_engn_mdl_cd, a.mnfctg_dt, a.orgnl_in_svc_dt, a.vvin_id',
          sep='')
txt=gsub('\n', ' ',txt)
sql(txt)


rm(list=ls())
q(save="yes")
