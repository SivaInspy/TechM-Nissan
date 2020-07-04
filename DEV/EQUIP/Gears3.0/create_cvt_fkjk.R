
##########################################################
#  Tracking :  CVT Part use for FK-JK
#  Customer: Nakai
#  Created by: J. Quiton
#  2018.03.15
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
spark_yarn_app_name_ <- 'sparkr-create_tracker_cvtfkjk'
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

#######################
# Create table
#######################

region=" ( glbl_mrkt_cd in ('USA','CAN') ) "
classcode= " ( cvrg_clas_cd in (0,8) ) "
cvtpart = " ( part5_nb in ('31020','3102M','310C0','310CM','310CN','31705','3170E') )"
L50 = " ( nml_prdctn_mdl_cd = 'L50' and prodmth <= 201303 and vhcl_yr_nb  in ('2013') )"
A34 = " ( nml_prdctn_mdl_cd = 'A34' and vhcl_yr_nb  in ('2007','2008') ) "
A35 = " ( nml_prdctn_mdl_cd = 'A35' and vhcl_yr_nb  in ('2009','2010','2011','2012','2013','2014') ) "
Z50 = " ( nml_prdctn_mdl_cd = 'Z50' and vhcl_yr_nb  in ('2003','2004','2005','2006','2007') ) "
Z51 = " ( nml_prdctn_mdl_cd = 'Z51' and vhcl_yr_nb  in ('2009','2010','2011','2012','2013','2014') ) "
EZ51 = " ( nml_prdctn_mdl_cd = 'EZ51' and vhcl_yr_nb  in ('2011','2012','2013','2014') ) "
E52 = " ( nml_prdctn_mdl_cd = 'E52' and vhcl_yr_nb  in ('2011','2012','2013','2014') ) "

F15 = " ( nml_prdctn_mdl_cd = 'F15' and vhcl_yr_nb  in ('2011','2012','2013','2014') ) "
B16 = " ( nml_prdctn_mdl_cd = 'B16' and vhcl_yr_nb  in ('2007','2008','2009','2010','2011','2012','2013','2014') ) "
S35 = " ( nml_prdctn_mdl_cd = 'S35' and vhcl_yr_nb  in ('2008','2009','2010','2011','2012','2013','2014','2015') ) "
M20 = " ( nml_prdctn_mdl_cd = 'M20' and vhcl_yr_nb  in ('2013','2014') ) "
CL32 = " ( nml_prdctn_mdl_cd = 'CL32' and vhcl_yr_nb  in ('2008','2009','2010','2011','2012','2013') ) "
L32 = " ( nml_prdctn_mdl_cd = 'L32' and vhcl_yr_nb  in ('2007','2008','2009','2010','2011','2012') ) "

cvt_group=" CASE WHEN  nml_prdctn_mdl_cd in ('L50','A34','A35','Z50','Z51','EZ51','E52')
                 THEN 'JK-K1'
                 WHEN  nml_prdctn_mdl_cd in ('CL32','L32')
                 THEN 'FK/JK-K1'
             ELSE 'FK' END as cvt_group "

issue=paste(" ( ", paste(region,classcode,cvtpart,sep=' and '), " ) ")
models=paste(" ( ", paste(L50,A34,A35,Z50,Z51,EZ51,E52,
                          F15,B16,S35,M20,CL32,L32,sep= ' or '), " ) ")


##--- output #1: Create Part Use Table ------
v_condition= paste(issue,models,sep=' and ')
v_obj<-paste(cvt_group,", a.* ", sep="")
v_table<-paste(adhoc_prefix, "part a ",sep="")
txt<-paste("select ", v_obj, " from ", v_table, " where ", v_condition, sep="")
cvt_part<-sql(txt)
createOrReplaceTempView(cvt_part,"cvt_part")

v_table<-paste(adhoc_prefix,"tracking_CVTPartUse_FKJK",sep="")
v_cluster<-"nml_prdctn_mdl_cd, glbl_mrkt_cd, retail_state, wrnty_clm_nb"
create_hive(bucket=40, partition="cvt_group",
            hivetable=v_table, temptable="cvt_part",
            whereclause="",
            clusterclause=v_cluster)

#--- output #2:  Create Claim table ----------#
v_table<-paste(adhoc_prefix,"tracking_CVTPartUse_FKJK",sep="")
txt=paste(" SELECT DISTINCT  wrnty_clm_nb, cvt_group from ", v_table,sep="")
cvt_claimnb=sql(txt)
createOrReplaceTempView(cvt_claimnb,"cvt_claimnb")

v_obj= " CURRENT_DATE() as Tracking_ReportDate, a.cvt_group, b.* "
v_table= paste(" cvt_claimnb a, ", adhoc_prefix,"claim b ",sep="")
v_condition = " a.wrnty_clm_nb = b.wrnty_clm_nb "
txt=paste(' SELECT ', v_obj, ' from  ', v_table, ' where ' , v_condition)
txt=gsub("\n"," ",txt)
cvt_claims<-sql(txt)
createOrReplaceTempView(cvt_claims,"cvt_claims")

v_table<-paste(adhoc_prefix,"tracking_CVTClaims_FKJK",sep="")
v_cluster<-"nml_prdctn_mdl_cd, glbl_mrkt_cd, retail_state, wrnty_clm_nb"

create_hive(bucket=40, partition="cvt_group",
            hivetable=v_table, temptable="cvt_claims",
            whereclause="",
            clusterclause=v_cluster)
