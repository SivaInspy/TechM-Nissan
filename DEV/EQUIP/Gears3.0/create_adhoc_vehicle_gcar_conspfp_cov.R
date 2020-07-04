#####################################################################
#
#  Creating Adhoc Vehicle x GCAR Consolidated PFP Coverage rates
#  Created by: Quiton, J
#
#  Create date: 2018.08.15
#  Update date: 2018.11.11
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
drive_req<-"rqst_base"
drive_vb<-"vhcl_base"
drive_adptn<-"adptn_data_base"
drive_adr<-"adr_base"
drive_cmdta<-"cmdta_base"
drive_model<-"mdl_cd_aray_base"
drive_pfp<-"adtnl_pfp_aray_base"
raw_prefix<-paste(db_name,".adhoc_raw_",sep="")
adhoc_prefix<-paste(db_name,".adhoc_",sep="")
spark_yarn_app_name_ <- paste('sparkr-create-adhoc-vehicle-gcar-conspfp-cov', Sys.time())
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


#--- step 1:  Create vehicle table by mnfctg dt----
v_table<-paste(adhoc_prefix, "vehicle",sep="")
v_obj<-"nml_prdctn_mdl_cd, mnfctg_vhcl_plnt_cd,v_mnfctg_dt, count(1) as VehPop "
v_group<-"nml_prdctn_mdl_cd, mnfctg_vhcl_plnt_cd,v_mnfctg_dt"
v_cond<-" nml_prdctn_mdl_cd is not null  and
          mnfctg_vhcl_plnt_cd is not null
          and v_mnfctg_dt is not null "
txt<-paste("select ",v_obj," from ", v_table, " where ", v_cond, " group by ", v_group, " order by ", v_group,sep="")
vhcl_x<-sql(txt)
createOrReplaceTempView(vhcl_x,"vhcl_x")

#--- step 3: vhcl_x left join gcar on prod_cd, plant
# 2018.09.05:  removed published filter on yellow. past open item is still a valid coverage
gcar_obj<-"prjct_ky,
           project_ref_number,
           pub_date,
           CONSOLIDATED_PFP5,
           cv_coverage,
           req_number,
           ad_date,
           cm_effectiveness,
           cm_class,
           final_eff,
           case
              when cm_class= 'Y' then final_eff
              else 0
           end as yellow_cov,
           case
              when cm_class= 'G' and UNIX_TIMESTAMP(ad_date,'yyyy-MM-dd')-UNIX_TIMESTAMP(v_mnfctg_dt,'yyyy-MM-dd')>=0 then final_eff
              else 0
           end as green_cov
            "
v_table<-paste("vhcl_x a, ", adhoc_prefix, "gcareffectiveness b ", sep="")
v_cond<-"a.nml_prdctn_mdl_cd = b.nml_prdctn_mdl_cd
         and a.mnfctg_vhcl_plnt_cd =b.cv_plantcd
         and cm_class in ('Y','G') "

txt<-paste("select distinct a.*, ", gcar_obj, " from ", v_table, " where ", v_cond,sep="")

vhcl_gcar<-sql(txt)
createOrReplaceTempView(vhcl_gcar,"vhcl_gcar")

#--- summarize and roll up the coverages ------#
v_obj<-" nml_prdctn_mdl_cd,
         mnfctg_vhcl_plnt_cd,
         v_mnfctg_dt,
         VehPop,
         CONSOLIDATED_PFP5,
         max(green_cov)/100 as green_cov,
         (1-max(green_cov)/100)*max(yellow_cov)/100 as yellow_cov "
v_table<-"vhcl_gcar"

v_group<-"nml_prdctn_mdl_cd, mnfctg_vhcl_plnt_cd,v_mnfctg_dt,VehPop,CONSOLIDATED_PFP5"
txt<-paste(" select ", v_obj, " from ", v_table, " group by ", v_group,  sep="")
vhcl_gcar_conspfp_cov<-sql(txt)
createOrReplaceTempView(vhcl_gcar_conspfp_cov,"vhcl_gcar_conspfp_cov")

#---- write to bde
txt<-"select CURRENT_DATE() as RunDate, a.* from vhcl_gcar_conspfp_cov a "
final<-sql(txt)
createOrReplaceTempView(final,"final")

v_table<-paste(adhoc_prefix,"vehicle_gcar_conspfp_cov",sep="")
create_hive(bucket=100, partition="nml_prdctn_mdl_cd",
            hivetable=v_table,
            temptable="final",
            whereclause="",
            clusterclause="mnfctg_vhcl_plnt_cd, v_mnfctg_dt, CONSOLIDATED_PFP5")
rm(list=ls())
quit(save="yes")
