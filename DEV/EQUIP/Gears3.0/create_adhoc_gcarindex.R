#####################################################################
#  Creating Adhoc Gcar index tables
#  Created by: Quiton, J
#
#  Update date: 2019.10.31
#               Refactoring for DRIVE
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
spark_yarn_app_name_ <- 'sparkr-create_adhoc_gcarindex'
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

#############################################################################
#    Generate Project x  Model Index
##############################################################################
#-- step 01 create vehicle list
tstamp<-Sys.time()
v_table<-paste(adhoc_prefix, "vehicle_navi",sep="")
v_cond<-' trim(nml_prdctn_mdl_cd) !="" '

txt=paste('select nml_prdctn_mdl_cd, concat("(?<![A-Za-z0-9])(",nml_prdctn_mdl_cd,")(?![A-Za-z0-9])") as psearch
      from ', v_table,  ' where',  v_cond,
      ' group by nml_prdctn_mdl_cd
        order by nml_prdctn_mdl_cd',sep='')
vehicle_list=sql(txt)
createOrReplaceTempView(vehicle_list,"vehicle_list")

#txt<-paste("vehicle_list completed in ", Sys.time()-tstamp)
#print(txt)
#tstamp<-Sys.time()

#---step02 create gcar_projectxmodel
v_table<-paste(adhoc_prefix,"gcar_project a ", sep="")

# First pass: look at deails of concern
txt<-paste("select b.nml_prdctn_mdl_cd, a.prjct_ky, a.project_ref_number
            from ", v_table, " left outer join vehicle_list b
            on a.detailsofconcern rlike b.psearch
            where b.nml_prdctn_mdl_cd is not null
            group by b.nml_prdctn_mdl_cd, a.prjct_ky, a.project_ref_number")
set01<-sql(txt)
createOrReplaceTempView(set01,"set01")

# 2nd pass: model array:
v_table2<-paste(db_drive_source,".",drive_model," b ",sep="")
txt<-paste("select trim(b.mdl_cd_elmnt_tx) as nml_prdctn_mdl_cd, a.prjct_ky, a.project_ref_number
            from ", v_table, ",", v_table2,
            " where  parnt_tbl_typ_tx= 'PRJCT' and
              a.prjct_ky=b.mdl_cd_aray_ky
            group by  b.mdl_cd_elmnt_tx, a.prjct_ky, a.project_ref_number ",sep="")
set02<-sql(txt)
set02$nml_prdctn_mdl_cd<-trim(set02$nml_prdctn_mdl_cd, "\"")
createOrReplaceTempView(set02,"set02")

gcar_projectxmodel<-sql(" select a.* from set01 a union select b.* from set02 b")
createOrReplaceTempView(gcar_projectxmodel,"gcar_projectxmodel")

create_hive(bucket=10,
            partition="nml_prdctn_mdl_cd",
            hivetable=paste(adhoc_prefix,"gcar_projectxmodel",sep=""),
            temptable="gcar_projectxmodel",
             whereclause="",
             clusterclause="prjct_ky, project_ref_number")

############################################################################
#    Generate Project x  PFP
##############################################################################
#-- step 01 create PFP  list
txt=paste(' select trim(pfp_nb) as pfp5 from ', raw_prefix, 'pfp_crnt_vw
            where trim(pfp_nb) !=""
            group by trim(pfp_nb)', sep='')
pfp01=sql(txt)
createOrReplaceTempView(pfp01,"pfp01")

txt=paste(' select trim(pfp_nb) as pfp5 from ', raw_prefix, 'pfp_dm
            where trim(pfp_nb) !=""
            group by trim(pfp_nb)', sep='')
pfp02=sql(txt)
createOrReplaceTempView(pfp02,"pfp02")

txt=' select trim(pfp_nb) as pfp5 from  equip.pfp_dm
            where trim(pfp_nb) !=""
            group by trim(pfp_nb)'
pfp03=sql(txt)
createOrReplaceTempView(pfp03,"pfp03")

txt=' select trim(fqi_pfp_nb) as pfp5 from  equip.fqi_pfp
            where trim(fqi_pfp_nb) !=""
            group by trim(fqi_pfp_nb)'
pfp04=sql(txt)
createOrReplaceTempView(pfp04,"pfp04")

vseq=c("01","02","03","04")
txt0=c(rep(" union ",length(vseq)-1), "")
txt=paste("select * from pfp",vseq," ",txt0,sep="", collapse= " ")
txt=gsub("/n", " ", txt)
pfp_list=sql(txt)
persist_table(pfp_list,"pfp_list",cacheFlag=FALSE)

txt= ' select b.pfp5,  concat("(?<![A-Za-z0-9])(",b.pfp5,")(?![A-Za-z0-9])") as psearch,
       CASE WHEN isnull(c.cnsdtd_pfp_nb) THEN b.pfp5  ELSE c.cnsdtd_pfp_nb END as CONSOLIDATED_PFP5
       from pfp_list b
       LEFT OUTER JOIN EQUIP.fqi_pfp c
       ON b.pfp5=  c.fqi_pfp_nb
       group by  1, b.pfp5,  concat("(?<![A-Za-z0-9])(",b.pfp5,")(?![A-Za-z0-9])"),
                 CASE WHEN isnull(c.cnsdtd_pfp_nb) THEN b.pfp5  ELSE c.cnsdtd_pfp_nb END'
txt=gsub("\n"," ",txt)
adhoc_pfp_list<-sql(txt)
persist_table(adhoc_pfp_list, "adhoc_pfp_list",cacheFlag=TRUE)

create_hive(bucket=40,
            partition="",
            hivetable=paste(adhoc_prefix,"pfp_list",sep=""),
            temptable="adhoc_pfp_list",
             whereclause="",
             clusterclause=" pfp5, CONSOLIDATED_PFP5 ")

###############################################################################
#  step02 create gcar_projectxpfp
################################################################################
#-- step0: add pfp array table
v_table<-paste(db_drive_source,".",drive_pfp," a ",sep="")
txt<-paste("select a.adtnl_pfp_aray_ky, trim(a.parnt_tbl_typ_tx) as add_pfp5
            from ", v_table,
            " where  a.adtnl_pfp_elmnt_tx= 'PRJCT' ",sep="")
add_pfp5<-sql(txt)
add_pfp5$add_pfp5<-trim(add_pfp5$add_pfp5, "\"")
createOrReplaceTempView(add_pfp5,"add_pfp5")

#--step 02 create cartesian product with drive project
collect(sql("set spark.sql.shuffle.partitions=800"))

txt<-paste("select a.consolidated_pfp5, a.pfp5, a.psearch, b.prjct_ky, b.project_ref_number
            from ",adhoc_prefix, "pfp_list a, ", adhoc_prefix, "gcar_project b
            where a.TableFlag=b.TableFlag",sep="")
gcar_cart01<-sql(txt)
createOrReplaceTempView(gcar_cart01,"gcar_cart01")

create_hive(bucket=800,
            partition="",
            hivetable=paste(adhoc_prefix,"gcar_cart01",sep=""),
            temptable="gcar_cart01",
            whereclause="",
            clusterclause="consolidated_pfp5, pfp5, psearch, prjct_ky, project_ref_number ")

#-- step 03 add model code and details of concern to cartesian product
txt=paste(' select a.*, b.codpfp, b.codpfp_add,  b.detailsofconcern, c.add_pfp5
      from  ', adhoc_prefix, 'gcar_cart01 a, ', adhoc_prefix,'gcar_project b
            left join add_pfp5 c  on b.prjct_ky=c.adtnl_pfp_aray_ky
            where a.project_ref_number= b.project_ref_number ' ,sep='')
base=sql(txt)
createOrReplaceTempView(base,"base")

#-- step 04 execute regular expression to find the
txt=" select  case when a.codpfp rlike a.psearch then 1
                   else 0 end as flag01,
              case when a.codpfp_add rlike a.psearch then 1
                   else 0 end as flag02,
              case when a.detailsofconcern rlike a.psearch then 1
                   else 0 end as flag03,
              case when a.add_pfp5 rlike a.psearch then 1
                   else 0 end as flag04, a.*  from  base a "
txt=gsub("\n"," ",txt)
rgx=sql(txt)
createOrReplaceTempView(rgx,"rgx")

#-- step 05 refine rgx to get all the hits
txt="select * from rgx where flag01+flag02+flag03+flag04 > 0"
gcar_projectxpfp5=sql(txt)
createOrReplaceTempView(gcar_projectxpfp5,"gcar_projectxpfp5")

create_hive(bucket=200,
            partition="",
            hivetable=paste(adhoc_prefix,"gcar_projectxpfp5",sep=""),
            temptable="gcar_projectxpfp5",
            whereclause="",
            clusterclause=" a.consolidated_pfp5, a.prjct_ky, a.project_ref_number ")

#############################################################################
#    Generate Project x  Model x PFP
##############################################################################
txt<-paste("select a.nml_prdctn_mdl_cd, b.consolidated_pfp5, b.pfp5, b.prjct_ky, b.project_ref_number
            from ", adhoc_prefix, "gcar_projectxmodel a,", adhoc_prefix, "gcar_projectxpfp5 b
            where a.project_ref_number = b.project_ref_number
            group by a.nml_prdctn_mdl_cd, b.consolidated_pfp5, b.pfp5, b.prjct_ky, b.project_ref_number ", sep="")

adhoc_gcar_projectxmodelxpfp5<-sql(txt)
createOrReplaceTempView(adhoc_gcar_projectxmodelxpfp5,"adhoc_gcar_projectxmodelxpfp5")

create_hive(bucket=40,
            partition="nml_prdctn_mdl_cd",
            hivetable=paste(adhoc_prefix,"gcar_projectxmodelxpfp5",sep=""),
            temptable="adhoc_gcar_projectxmodelxpfp5",
            whereclause="",
            clusterclause=" a.consolidated_pfp5, a.pfp5, a.prjct_ky, a.project_ref_number ")
