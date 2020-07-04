#############################################################
#  Generate GCAR coverage table
#  Created by: A. Godbole  2018.03.21
#  Reviewed by:  S. Krishnamurthy,  J.Quiton
#  Revised:  Quiton J.
#            2018.10.05
#            Converted for loop via gapply() + trick to return
#            a 1:M table
############################################################
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
spark_yarn_app_name_ <- paste('sparkr-adhoc_gcarcoverage_',Sys.time())
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

##############################################
# Step 2: Initialize GCAR tables and schema
##############################################
txt<-paste("select prjct_ky,project_ref_number, detailsofconcern from ", adhoc_prefix,
           "gcar_project  where project_ref_number is not null",sep="")
gcars_project=sql(txt)

gcars_schema<- structType(structField("prjct_ky",                          "double",TRUE ),
                          structField("project_ref_number",                "string",TRUE ),
                          structField("cv_model",                          "string",TRUE ),
                          structField("cv_plantcd",                          "string",TRUE ),
                          structField("cv_pfp",                          "string",TRUE ),
                          structField("cv_coverage",                          "string",TRUE )
                          )
grp_columns<-c("prjct_ky","project_ref_number")

#########################################################################
# Step 3a: define gapply() function: get_coverage()
#          This functions strips the <detailsofconcern> field and
#          parse them into cv_model, cv_plantcd_, cv_pfp and cv_coverage
##########################################################################
get_coverage<-function(key,x){
  # initialize
            prjct_ky <- as.double(x$prjct_ky)
  project_ref_number <- as.character(x$project_ref_number)
            cv_model <- NA
          cv_plantcd <- NA
              cv_pfp <- NA
         cv_coverage <- NA
    detailsofconcern <- as.character(x$detailsofconcern)
                  m  <- gregexpr("\\[.*?\\]", detailsofconcern)
        temp_matches <- unlist(regmatches(detailsofconcern, m) )
              master <- NULL
  if (sum(is.na(temp_matches)==0))
    {

    for (j in 1: length(temp_matches))
    {
      temp_string = temp_matches[j]
      sc_index = unlist(gregexpr(pattern = "\\;+",temp_string, perl= TRUE))
          if (sc_index[1]== -1)
         {
            temp_row = as.data.frame(list(prjct_ky=prjct_ky, project_ref_number=project_ref_number))
            start_index = 2
            stop_index =unlist(gregexpr(pattern = "\\]",temp_string, perl= TRUE))
            hp_index =unlist(gregexpr(pattern = "\\-+",temp_string, perl= TRUE))

            if ((length(unlist(hp_index))) > 2) {
              temp_row["cv_model"] <- substring(temp_string, start_index, hp_index[1] -1)
              t1=substring(temp_string, hp_index[1]+1, hp_index[2]-1)
              t2=substring(temp_string, hp_index[2]+1, hp_index[3]-1)
              #-- correcting for wrong pfp and plant coding order -------#
              if (nchar(t1)==1){
                      f1=t1
                      f2=t2
              } else {
                      f1=t2
                      f2=t1
              }
              temp_row["cv_plantcd"] <- f1
              temp_row["cv_pfp"] <- f2
              temp_row["cv_coverage"] <- 100
            }
            else{
              temp_row["cv_model"] <- substring(temp_string, start_index, hp_index[1] -1)
              temp_row["cv_plantcd"] <- 'ALL'
              temp_row["cv_pfp"] <- substring(temp_string, hp_index[1]+1, hp_index[2]-1)
              temp_row["cv_coverage"] <- 100
            }

            master<-rbind(master,temp_row)

          }
          else{
          startsub_index = 2
          stopsub_index = sc_index[1]
          for (l in 1:(length(sc_index)+1))
               {
                 temp_substring = substring(temp_string,startsub_index,stopsub_index)
                 hp_index =unlist(gregexpr(pattern = "\\-+",temp_substring, perl= TRUE))
                 start_index= 1
                 if  (l == length(sc_index)+1)
               {

                   stop_index = unlist(gregexpr(pattern = "\\]",temp_substring, perl= TRUE))
                 }
                 else{
                   stop_index = unlist(gregexpr(pattern = "\\;",temp_substring, perl= TRUE))
                 }
                 temp_row = as.data.frame(list(prjct_ky=prjct_ky, project_ref_number=project_ref_number))
                 if ((length(unlist(hp_index))) > 2) {
                   temp_row["cv_model"] <- substring(temp_substring, start_index, hp_index[1] -1)
                   t1=substring(temp_substring, hp_index[1]+1, hp_index[2]-1)
                   t2=substring(temp_substring, hp_index[2]+1, hp_index[3]-1)
                   #-- correcting for wrong pfp and plant coding order -------#
                   if (nchar(t1)==1){
                      f1=t1
                      f2=t2
                   } else {
                      f1=t2
                      f2=t1
                   }
                   temp_row["cv_plantcd"] <- f1
                   temp_row["cv_pfp"] <- f2
                   temp_row["cv_coverage"] <- 100
                 }
                 else{
                   temp_row["cv_model"] <- substring(temp_substring, start_index, hp_index[1] -1)
                   temp_row["cv_plantcd"] <- "ALL"
                   temp_row["cv_pfp"] <- substring(temp_substring, hp_index[1]+1, hp_index[2]-1)
                   temp_row["cv_coverage"] <- 100
                 }
                 startsub_index = sc_index[l]+1
                 if (l != length(sc_index))
                 {stopsub_index = sc_index[l+1]}
                 else{
                   stopsub_index = unlist(gregexpr(pattern = "\\]",temp_string, perl= TRUE))
                 }
                master= rbind(master,temp_row)
          }
        }
      }
                         prjct_ky<-as.double(master$prjct_ky)
               project_ref_number<-as.character(master$project_ref_number)
                         cv_model<-as.character(master$cv_model)
                       cv_plantcd<-as.character(master$cv_plantcd)
                           cv_pfp<-as.character(master$cv_pfp)
                      cv_coverage<-as.character(master$cv_coverage)

    }
               return(data.frame(prjct_ky, project_ref_number,cv_model,cv_plantcd,cv_pfp,cv_coverage,stringsAsFactors=FALSE))
}

##################################################################################
# Step 3B: Calling gapply() and create the IR temporary table
##################################################################################
gc<- gapply(gcars_project,grp_columns,get_coverage,gcars_schema)
createOrReplaceTempView(gc,"gc")

##################################################################################
# Step 4: Data cleansing:
#         The parsing algorithm may have picked up garbage output.  This
###################################################################################
v_table<-paste(adhoc_prefix,"vehicle_navi ",sep="")
v_group<-" nml_prdctn_mdl_cd, mnfctg_vhcl_plnt_cd "
v_cond<-" mnfctg_vhcl_plnt_cd != 'USA'"
txt=paste (" SELECT ", v_group, " from ", v_table, " where ", v_cond, " group by ", v_group, sep="")

model_plant=sql(txt)
createOrReplaceTempView(model_plant, "model_plant")

pub_date <- ' TO_DATE(substr(d.pubdate,1,10), "yyyy-MM-dd") as pub_date '
pub_agemth<- ' ROUND(DATEDIFF(CURRENT_DATE(),
                              TO_DATE(substr(d.pubdate,1,10), "yyyy-MM-dd") )/30.4, 2)  as pub_agemth '

v_obj= paste(' a.prjct_ky, a.project_ref_number, ', pub_date, ',', pub_agemth, ', a.cv_model as nml_prdctn_mdl_cd,
        ( CASE WHEN a.cv_plantcd in ("ALL", "A")
              THEN b.mnfctg_vhcl_plnt_cd
              else a.cv_plantcd
          END ) as cv_plantcd,
        ( CASE WHEN isnull(c.cnsdtd_pfp_nb)
              THEN a.cv_pfp
              ELSE c.cnsdtd_pfp_nb
          END ) as CONSOLIDATED_PFP5,
        a.cv_pfp,
       ( CASE WHEN a.cv_coverage  <=1
             THEN  a.cv_coverage*100
             ELSE  a.cv_coverage
        END ) as cv_coverage ' )

v_table= ' gc a '

v_outer = paste(' LEFT OUTER JOIN model_plant b
               on a.cv_model = b.nml_prdctn_mdl_cd
            LEFT OUTER JOIN equip.fqi_pfp c
               ON a.cv_pfp=  c.fqi_pfp_nb
            LEFT OUTER JOIN ', adhoc_prefix, 'gcar_project d
               ON a.project_ref_number = d. project_ref_number',sep='')

v_cond = ' trim(nml_prdctn_mdl_cd) != "" '

txt=paste(' SELECT DISTINCT ', v_obj, ' from ', v_table, v_outer, ' WHERE ', v_cond)
txt=gsub('\n', ' ', txt)
gc_final=sql(txt)
createOrReplaceTempView(gc_final, "gc_final")

##################################################################################
# Step 5: Check for project keys not covered by the parsing algorithm
###################################################################################
covered_proj<-sql("select prjct_ky from gc_final group by prjct_ky")
createOrReplaceTempView(covered_proj,"covered_proj")

pub_date <- ' TO_DATE(substr(b.pubdate,1,10), "yyyy-MM-dd") as pub_date '
pub_agemth<- ' ROUND(DATEDIFF(CURRENT_DATE(),
                              TO_DATE(substr(b.pubdate,1,10), "yyyy-MM-dd") )/30.4, 2)  as pub_agemth '

v_table<-paste(adhoc_prefix,"gcar_projectxmodelxpfp5 a, ", adhoc_prefix, "gcar_project b ",  sep="")
v_obj<-paste("a.prjct_ky, a.project_ref_number, ", pub_date, ", ", pub_agemth, ", a.nml_prdctn_mdl_cd,
              d.mnfctg_vhcl_plnt_cd as cv_plantcd, a.consolidated_pfp5 as CONSOLIDATED_PFP5, a.pfp5 as cv_pfp, 100 as cv_coverage ",sep="")

v_leftouter<-" left outer join covered_proj c on a.prjct_ky=c.prjct_ky
               left outer join model_plant d on a.nml_prdctn_mdl_cd=d.nml_prdctn_mdl_cd "

v_cond<-"  a.prjct_ky= b.prjct_ky and c.prjct_ky is null "
txt<-paste("select distinct ", v_obj, " from ", v_table, v_leftouter, " where ", v_cond, sep="")
notcovered_proj<-sql(txt)
createOrReplaceTempView(notcovered_proj, "notcovered_proj")

txt<-"select * from gc_final union select * from notcovered_proj where CONSOLIDATED_PFP5 is not null"
gc_final02<-sql(txt)
createOrReplaceTempView(gc_final02,"gc_final02")

#---write to BDE: adhoc_gcarcoverage ---------#
create_hive(bucket=40, partition="cv_plantcd", hivetable=paste(adhoc_prefix,"gcarcoverage",sep=""),
             temptable="gc_final02 ",
             whereclause="",
             clusterclause="a.nml_prdctn_mdl_cd, a.CONSOLIDATED_PFP5, a.prjct_ky, a.project_ref_number, a.pub_date ")

v_table=" gc_final02 a "
v_leftouter<-paste("
      LEFT OUTER JOIN ", adhoc_prefix, "gcar_req b
         ON a.prjct_ky= b.prjct_ky
      LEFT OUTER JOIN ", adhoc_prefix, "gcar_cd c
         ON b.rqst_ky = c.rqst_ky
      LEFT OUTER JOIN ", adhoc_prefix, "gcar_ad_date d
         ON c.cmdta_ky = d.cmdta_ky ", sep="")

v_group= " a.nml_prdctn_mdl_cd, a.CONSOLIDATED_PFP5 "

ad_date <- " TO_DATE(substr(d.ad_date,1,10), 'yyyy-MM-dd') as ad_date "

baseline_date<- " ( CASE WHEN trim(d.ad_date)!=''
                       THEN TO_DATE(substr(d.ad_date,1,10), 'yyyy-MM-dd')
                       ELSE a.pub_date
                   END ) as baseline_date "

cmclass= " (CASE WHEN ( trim(d.ad_date)!='' ) THEN 'G'
                 WHEN ( (d.ad_date is NULL or trim(d.ad_date) =='') ) THEN 'Y'
                 ELSE 'X'
                 END ) as cm_class "

v_obj = paste(" monotonically_increasing_id() as ID, a.*,  b.rqst_ky, b.request_ref_number as req_number,  b.status as request_status,
                c.cmdta_ky, ", ad_date, ", ",baseline_date, " , 100 as cm_effectiveness, 100 as final_eff, ", cmclass)
txt=paste(" SELECT  ", v_obj, " FROM ", v_table, v_leftouter)
txt=gsub(" \n", " ", txt)
gc_finalB=sql(txt)
createOrReplaceTempView(gc_finalB,"gc_finalB")

#---write to BDE: equip.adhoc_gcareffectiveness -----#
v_table<-paste(adhoc_prefix,"gcareffectiveness",sep="")
create_hive(bucket=40, partition="cv_plantcd", hivetable=v_table,
             temptable="gc_finalB ",
             whereclause="",
             clusterclause="a.nml_prdctn_mdl_cd, a.CONSOLIDATED_PFP5, a.cm_class, a.ad_date, a.pub_date,
                            a.prjct_ky, a.project_ref_number, a.rqst_ky, a.req_number, a.cmdta_ky ")
