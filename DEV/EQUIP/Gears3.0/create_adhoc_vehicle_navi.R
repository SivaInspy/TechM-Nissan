#####################################################################
#  Creating Adhoc Vehicle Navi
#  for NNA_TCS_COE
#
#  Created by: Quiton, J
#  Modified by: Quiton, J
#  Update date: 2019.10.14
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
db_name<-'EQUIP'
raw_prefix<-paste(db_name,'.adhoc_raw_',sep='')
adhoc_prefix<-paste(db_name,'.adhoc_',sep='')

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

spark_yarn_app_name_ <- 'sparkr-create_adhoc_vehicle_navi'
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


####################
# vehicle_navi
#####################
vfilter=" a.vhcl_yr_nb, a.vhcl_make, a.nml_prdctn_mdl_cd, a.glbl_mrkt_cd, a.mnfctg_vhcl_plnt_cd, a.plant_name,
          a.vhcl_flt_in,  a.nna_sls_mdl_cd, a.vhcl_engn_mdl_cd "

measures=" min(a.v_mnfctg_dt) as min_prod_date, max(a.v_mnfctg_dt) as max_prod_date,
           min(a.v_orgnl_in_svc_dt) as min_orgnl_insvcdate, max(a.v_orgnl_in_svc_dt) as max_orgnl_insvcdate,
           count(distinct a.vvin_id) as VehiclePop, sum(CASE WHEN sold_flag = 'YES' THEN 1 ELSE 0 END) as VehicleSoldPop "
measures=gsub("\n", " ",measures)

txt=paste(" SELECT DISTINCT CURRENT_DATE() as RunDate, ", vfilter," , ", measures,
           " from ", adhoc_prefix,"vehicle a group by ", vfilter,sep="")
txt=gsub("\n", " ",txt)
vtemp<-sql(txt)
createOrReplaceTempView(vtemp,"vtemp")

#---Create navi table -----
v_table<-paste(adhoc_prefix,'vehicle_navi',sep='')
create_hive(bucket=4, partition="", hivetable=v_table, temptable="vtemp",
             whereclause="",
             clusterclause=vfilter)

####################
# vehicle_my
#####################
txt=paste(" SELECT vhcl_yr_nb, nml_prdctn_mdl_cd, vhcl_engn_mdl_cd from ", adhoc_prefix, "vehicle
                     group by vhcl_yr_nb, nml_prdctn_mdl_cd, vhcl_engn_mdl_cd ",sep="")
vehicle_my<-sql(txt)
createOrReplaceTempView(vehicle_my,"vehicle_my")

v_cluster<-" vhcl_yr_nb, nml_prdctn_mdl_cd, vhcl_engn_mdl_cd  "
v_table<-paste(adhoc_prefix,"vehicle_my",sep="")

create_hive(bucket=4, partition="", hivetable=v_table, temptable="vehicle_my",
             whereclause="",
             clusterclause=v_cluster)

####################
# modelyear
#####################
txt=paste(" SELECT vhcl_yr_nb  from ", adhoc_prefix, "vehicle
                   group by vhcl_yr_nb order by vhcl_yr_nb desc ",sep="")
modelyear<-sql(txt)
createOrReplaceTempView(modelyear,"modelyear")

v_table<-paste(adhoc_prefix,"modelyear",sep="")
create_hive(bucket=1, partition="", hivetable=v_table, temptable="modelyear",
             whereclause="",
             clusterclause="")

vfilter,sep="")
txt=gsub("\n", " ",txt)
vtemp<-sql(txt)
createOrReplaceTempView(vtemp,"vtemp")

#---Create navi table -----
v_table<-paste(adhoc_prefix,'vehicle_navi',sep='')
create_hive(bucket=4, partition="", hivetable=v_table, temptable="vtemp",
             whereclause="",
             clusterclause=vfilter)

####################
# vehicle_my
#####################
txt=paste(" SELECT vhcl_yr_nb, nml_prdctn_mdl_cd, vhcl_engn_mdl_cd from ", adhoc_prefix, "vehicle
                     group by vhcl_yr_nb, nml_prdctn_mdl_cd, vhcl_engn_mdl_cd ",sep="")
vehicle_my<-sql(txt)
createOrReplaceTempView(vehicle_my,"vehicle_my")

v_cluster<-" vhcl_yr_nb, nml_prdctn_mdl_cd, vhcl_engn_mdl_cd  "
v_table<-paste(adhoc_prefix,"vehicle_my",sep="")

create_hive(bucket=4, partition="", hivetable=v_table, temptable="vehicle_my",
             whereclause="",
             clusterclause=v_cluster)

####################
# modelyear
#####################
txt=paste(" SELECT vhcl_yr_nb  from ", adhoc_prefix, "vehicle
                   group by vhcl_yr_nb order by vhcl_yr_nb desc ",sep="")
modelyear<-sql(txt)
createOrReplaceTempView(modelyear,"modelyear")

v_table<-paste(adhoc_prefix,"modelyear",sep="")
create_hive(bucket=1, partition="", hivetable=v_table, temptable="modelyear",
             whereclause="",
             clusterclause="")
