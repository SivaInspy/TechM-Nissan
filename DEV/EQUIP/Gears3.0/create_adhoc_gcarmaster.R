#####################################################################
#
#  Creating Adhoc Gcar Master Table
#  Created by: Quiton, J
#
#  Update date: 2019.11.08
#               Optimized orc tables via cluster
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
spark_yarn_app_name_ <- paste('sparkr-create_adhoc_gcarmaster', Sys.time())
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


v_table <- paste(adhoc_prefix,"gcar_project a",sep="")
v_fields <- paste(" CURRENT_DATE() as ReportDate,
        a.mdl_cd_aray_in,  a.prjct_ky, a.project_ref_number as project_number,
        b.rqst_ky, b.request_ref_number as req_number, b.request_type,
        a.subject,
        b.status as request_status,
        concat(a.codpfp,  ' ', a.codpfp_add, ' ', a.detailsofconcern) as expanded_detailsofconcern,
        c.cmdta_ky,
        c.cmdta_ky as cd_number,
        c.cm_type,
        100 as cm_effectiveness,
        d.ad_date ", sep="")

#--- left outer joins
v_leftouter<-paste("
  LEFT OUTER JOIN ", adhoc_prefix, "gcar_req b
    ON a.prjct_ky= b.prjct_ky
  LEFT OUTER JOIN ", adhoc_prefix, "gcar_cd c
    ON b. rqst_ky = c.rqst_ky
  LEFT OUTER JOIN ", adhoc_prefix, "gcar_ad_date d
    ON c.cmdta_ky = d.cmdta_ky ",sep="")


#---building the sql statement
txt=paste('  SELECT DISTINCT',
           v_fields,' FROM ',v_table, v_leftouter)
gcarmaster=sql(txt)
createOrReplaceTempView(gcarmaster, "gcarmaster")

#--- merge with pfp -----

v_table<-paste(adhoc_prefix, "gcar_projectxmodelxpfp5 a, gcarmaster b",sep="")
v_cond<-"a.prjct_ky = b.prjct_ky"
txt= paste(" SELECT a.nml_prdctn_mdl_cd, a.consolidated_pfp5, a.pfp5, b.*
      from ", v_table, " where ", v_cond, sep="")
gcarmaster2=sql(txt)
createOrReplaceTempView(gcarmaster2, "gcarmaster2")

#---- final table ---------
v_obj = " a.*,  b.cv_plantcd, b.cv_coverage, c.pfp_ds as consolidated_pfp5_desc "
v_table =" gcarmaster2 a "
v_leftouter = paste(" LEFT OUTER JOIN ", adhoc_prefix, "gcarcoverage b ON
                           a.prjct_ky = b.prjct_ky AND
                           a.nml_prdctn_mdl_cd = b.nml_prdctn_mdl_cd
                      LEFT OUTER JOIN equip.pfp_dm c ON
                           a.consolidated_pfp5=c.pfp_nb",sep="")

txt=paste(" SELECT ", v_obj, " from ",v_table, v_leftouter,sep="")
gcarmaster3<-sql(txt)
createOrReplaceTempView(gcarmaster3,"gcarmaster3")

#-- write to BDE
v_table<-paste(adhoc_prefix,"gcarmaster",sep="")
create_hive(bucket=40, partition="cv_plantcd", hivetable=v_table,
             temptable="gcarmaster3 ",
             whereclause="",
             clusterclause="a.nml_prdctn_mdl_cd, a.cv_plantcd, a.CONSOLIDATED_PFP5,
                            a.prjct_ky, a.project_number, a.rqst_ky, a.req_number, a.cmdta_ky, a.ad_date  ")
