#####################################################################
#  Creating Adhoc Vehicle
#  for NNA_TCS_COE
#
#  Created by: Quiton, J
#  Modified by: Quiton, J
#
#  Update date: 2019.10.14
#               increased vmis indicator to 60
#  Note: IF you are adding fields, please set CREATE_TABLE_FLAG=TRUE for the first run
#        and then set it to FALSE to do insert /overwrite
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
spark_yarn_app_name_ <- 'sparkr-create_coe_adhoc_vehicle'
loglevel <- 'ERROR'

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

###########################################
#  main raw vehicle table
######################################
v_table <- paste(raw_prefix,'vhcl a',sep='')

#--- selecting vehicle fields ------
adj_orgnl_INSVCDT<-' CASE WHEN (a.orgnl_in_svc_dt is null or substr(a.orgnl_in_svc_dt,1,10)="01/01/0001")
                               THEN  CASE WHEN (a.in_svc_dt is null or substr(a.in_svc_dt,1,10)="01/01/0001")
                                          THEN NULL
                                     ELSE TO_DATE(substr(a.in_svc_dt,1,10),"MM/dd/yyyy")
                                     END
                               ELSE TO_DATE(substr(a.orgnl_in_svc_dt,1,10),"MM/dd/yyyy")
                      END '

adj_orgnl_INSVCDT2<-' CASE WHEN (a.orgnl_in_svc_dt is null or substr(a.orgnl_in_svc_dt,1,10)="01/01/0001")
                               THEN CASE WHEN (a.in_svc_dt is null or substr(a.in_svc_dt,1,10)="01/01/0001")
                                          THEN ""
                                    ELSE substr(a.in_svc_dt,1,10)
                                    END
                               ELSE substr(a.orgnl_in_svc_dt,1,10)
                      END '

adj_orgnl_INSVCMTH<-' CASE WHEN (a.orgnl_in_svc_dt is null or substr(a.orgnl_in_svc_dt,1,10)="01/01/0001")
                                THEN CASE WHEN (a.in_svc_dt is null or substr(a.in_svc_dt,1,10)="01/01/0001")
                                          THEN ""
                                     ELSE concat(substr(a.in_svc_dt,7,4),substr(a.in_svc_dt,1,2))
                                     END
                               ELSE concat(substr(a.orgnl_in_svc_dt,7,4),substr(a.orgnl_in_svc_dt,1,2))
                      END '

v_orgnl_in_svc_dt<- paste(adj_orgnl_INSVCDT, ' as v_orgnl_in_svc_dt ',sep="")

v_fctry_ivntry_trnsfr_dt<-' TO_DATE(substr(a.FCTRY_IVNTRY_TRNSFR_DT, 1,10),"MM/dd/yyyy") as v_fctry_ivntry_trnsfr_dt '

v_orgnl_whlsl_dt<-' TO_DATE(substr(a.orgnl_whlsl_dt, 1,10),"MM/dd/yyyy") as v_orgnl_whlsl_dt'

v_actl_dlr_dlvry_dt<-' TO_DATE(substr(a.actl_dlr_dlvry_dt, 1,10),"MM/dd/yyyy") as v_actl_dlr_dlvry_dt'

v_mnfctg_dt<- ' TO_DATE(substr(a.mnfctg_dt,1,10),"MM/dd/yyyy") as v_mnfctg_dt'

# Note datediff(END_DATE_string, START_DATE_string) in Spark 2.0

#  Note: in service date >= whole sale date
dlr_s01= paste(' DATEDIFF(',adj_orgnl_INSVCDT,
               ',TO_DATE(substr(a.orgnl_whlsl_dt,1,10), "MM/dd/yyyy") )', sep="")

#  Note: Current date >= whole sale date
dlr_s02= ' DATEDIFF(CURRENT_DATE(),
                   TO_DATE(substr(a.orgnl_whlsl_dt,1,10), "MM/dd/yyyy") ) '

#  Note: in service date >= whole sale date
dlr_s03= paste(' FLOOR(DATEDIFF(',adj_orgnl_INSVCDT,
               ',TO_DATE(substr(a.orgnl_whlsl_dt,1,10), "MM/dd/yyyy"))/30.4) ',sep="")

#  Note: Current date >= whole sale date
dlr_s04= ' FLOOR(DATEDIFF(CURRENT_DATE(),
                          TO_DATE(substr(a.orgnl_whlsl_dt,1,10), "MM/dd/yyyy"))/30.4) '


storage1<- ' ( CASE WHEN TO_DATE(substr(a.FCTRY_IVNTRY_TRNSFR_DT, 1,10),"MM/dd/yyyy") >=
                         TO_DATE(substr(a.mnfctg_dt,1,10),"MM/dd/yyyy") and
                         TO_DATE(substr(a.mnfctg_dt,1,10),"MM/dd/yyyy") >
                         TO_DATE("01/01/1900","MM/dd/yyyy")
                  THEN DATEDIFF(TO_DATE(substr(a.FCTRY_IVNTRY_TRNSFR_DT, 1,10),"MM/dd/yyyy") ,
                       TO_DATE(substr(a.mnfctg_dt,1,10),"MM/dd/yyyy") )
                  WHEN ( TO_DATE(substr(a.FCTRY_IVNTRY_TRNSFR_DT, 1,10),"MM/dd/yyyy") = TO_DATE("01/01/1900","MM/dd/yyyy")
                         or a.FCTRY_IVNTRY_TRNSFR_DT is null) AND
                       TO_DATE(substr(a.mnfctg_dt,1,10),"MM/dd/yyyy") > TO_DATE("01/01/1900","MM/dd/yyyy")
                  THEN DATEDIFF(CURRENT_DATE(),
                       TO_DATE(substr(a.mnfctg_dt,1,10),"MM/dd/yyyy") )
                  ELSE NULL
               END ) as storage1 '

storage2<- ' ( CASE WHEN TO_DATE(substr(a.actl_dlr_dlvry_dt, 1,10),"MM/dd/yyyy") >=
                         TO_DATE(substr(a.FCTRY_IVNTRY_TRNSFR_DT,1,10),"MM/dd/yyyy") and
                         TO_DATE(substr(a.FCTRY_IVNTRY_TRNSFR_DT,1,10),"MM/dd/yyyy") >
                         TO_DATE("01/01/1900","MM/dd/yyyy")
                  THEN DATEDIFF(TO_DATE(substr(a.actl_dlr_dlvry_dt, 1,10),"MM/dd/yyyy") ,
                       TO_DATE(substr(a.FCTRY_IVNTRY_TRNSFR_DT,1,10),"MM/dd/yyyy") )
                  WHEN ( TO_DATE(substr(a.actl_dlr_dlvry_dt, 1,10),"MM/dd/yyyy") = TO_DATE("01/01/1900","MM/dd/yyyy")
                         or a.actl_dlr_dlvry_dt is null) AND
                       TO_DATE(substr(a.FCTRY_IVNTRY_TRNSFR_DT,1,10),"MM/dd/yyyy") > TO_DATE("01/01/1900","MM/dd/yyyy")
                  THEN DATEDIFF(CURRENT_DATE(),
                       TO_DATE(substr(a.FCTRY_IVNTRY_TRNSFR_DT,1,10),"MM/dd/yyyy") )
                  ELSE NULL
               END ) as storage2 '

storage3<- paste(' ( CASE WHEN (', adj_orgnl_INSVCDT, ') >=
                         TO_DATE(substr(a.actl_dlr_dlvry_dt,1,10),"MM/dd/yyyy") and
                         TO_DATE(substr(a.actl_dlr_dlvry_dt,1,10),"MM/dd/yyyy") >
                         TO_DATE("01/01/1900","MM/dd/yyyy")
                  THEN DATEDIFF( (', adj_orgnl_INSVCDT, ') ,
                       TO_DATE(substr(a.actl_dlr_dlvry_dt,1,10),"MM/dd/yyyy") )
                  WHEN ( (', adj_orgnl_INSVCDT, ') = TO_DATE("01/01/1900","MM/dd/yyyy")
                         or (', adj_orgnl_INSVCDT, ') is null ) AND
                       TO_DATE(substr(a.actl_dlr_dlvry_dt,1,10),"MM/dd/yyyy") > TO_DATE("01/01/1900","MM/dd/yyyy")
                  THEN DATEDIFF(CURRENT_DATE(),
                       TO_DATE(substr(a.actl_dlr_dlvry_dt,1,10),"MM/dd/yyyy") )
                  ELSE NULL
               END ) as storage3 ', sep='')


#  Note: In service date >= manufacturing date
sold_condition= paste(' DATEDIFF(', adj_orgnl_INSVCDT,
                      ',TO_DATE(substr(a.mnfctg_dt,1,10), "MM/dd/yyyy")) >=0 ',sep="")


sold_flag<- paste(' CASE WHEN (', sold_condition, ') THEN "YES" ELSE "NO" END as sold_flag',sep='')

long_whlsl_flag<- ' ( CASE WHEN  DATEDIFF(TO_DATE(substr(a.orgnl_whlsl_dt, 1,10),"MM/dd/yyyy"), TO_DATE(substr(a.mnfctg_dt,1,10),"MM/dd/yyyy") ) > 180 and
                                 TO_DATE(substr(a.mnfctg_dt,1,10),"MM/dd/yyyy") > TO_DATE("01/01/1900","MM/dd/yyyy")  and
                                  a.orgnl_whlsl_dt is not null
                           THEN  "Y"
                           ELSE  "N"
                        END ) as long_whlsl_flag '

dlr_storage_days<-paste(' CASE WHEN (', sold_condition, ') THEN ', dlr_s01, ' ELSE ' , dlr_s02, ' END as dlr_storage_days ',sep='')

dlr_storage_months<-paste(' CASE WHEN (', sold_condition, ') THEN ', dlr_s03, ' ELSE ' , dlr_s04, ' END as dlr_storage_months ',sep='')

v_mis <- paste(' FLOOR(DATEDIFF(CURRENT_DATE(),', adj_orgnl_INSVCDT, ')/30.4) ', sep="")

v_mis_n <- paste(' DATEDIFF(CURRENT_DATE(),', adj_orgnl_INSVCDT, ')/30.4 ', sep="")


#---- v_mis 0-180---#
v_mis_seq<- 1:180
txt0<-paste(' CASE WHEN (', v_mis, '>=',v_mis_seq, ') THEN 1 ELSE 0 END as vmis',v_mis_seq,sep='',collapse= ',')
v_mis_vec<-paste(' CASE WHEN (', sold_condition, ') THEN 1 ELSE 0 END as vmis0, ', txt0, sep='')

v_fields <- paste(' CURRENT_DATE() as ReportDate,
        1 as TableFlag, a.ls_up_tm_stmp as vehicle_last_time_stamp,
        TO_DATE(substr(a.crte_tm_stmp, 1,10), "MM/dd/yyyy") as create_date,
        a.vin_id as vvin_id,
        substr(a.vhcl_yr_nb,1,4) as vhcl_yr_nb,
        a.vhcl_mk_cd as vhcl_make,
        a.nml_prdctn_mdl_cd,
        a.nml_prdctn_mdl_cd as vehprodcd,
        trim(upper(a.vhcl_ln_nm)) as  vhcl_ln_nm,
        a.glbl_mrkt_cd,
        a.asct_dstrbr_cd,
        a.fclty_dstrbr_cd,
        c.profile,
        c.mu,
        c.sigma,
        a.mnfctg_vhcl_plnt_cd,
        b.vhcl_mnfctr_plnt_nm as plant_name,
        substr(a.mnfctg_dt,1,10) as mnfctg_dt,
        concat(substr(a.mnfctg_dt,7,4),substr(a.mnfctg_dt,1,2)) as prodmth, ',
        adj_orgnl_INSVCMTH,' as insvcmth,
        a.rtl_sl_lse_dt, ',
        adj_orgnl_INSVCDT2,' as orgnl_in_svc_dt,
        substr(a.in_svc_dt,1,10) as in_svc_dt',
        ',',v_orgnl_in_svc_dt,
        ',',v_actl_dlr_dlvry_dt,
        ',',v_orgnl_whlsl_dt,
        ',',v_fctry_ivntry_trnsfr_dt,
        ',',v_mnfctg_dt,
        ',',storage1,
        ',',storage2,
        ',',storage3,
        ',',long_whlsl_flag,
         ',',dlr_storage_days,
        ',',dlr_storage_months,
        ',',v_mis,' as v_mis ',
        ',',v_mis_n,' as v_mis_n ',
        ', a.vhcl_flt_in,
        a.asgnd_load_in,
        a.cv_in,
        a.us_spcfcn_in,
        a.vhcl_blmt_shpmnt_in,
        a.vhcl_crshd_in,
        a.vhcl_frgmt_in,
        a.vhcl_hld_in,
        a.vhcl_rtrn_in,
        a.vhcl_flt_cntct_nm,
        a.vhcl_ownr_nm,
        a.engn_prfx_8_dgt_cd,
        a.vhcl_idfctn_4_dgt_nb,
        a.tlmtcs_svc_vndr_cd,
        a.navi_plnt_scan_ts,
        a.navi_id,
        a.sim_id,
        a.drv_trn_ds,
        a.btry_12_volt_srl_nb,
        a.trm_cd as intr_clr_cd,
        a.trm_cd_nm as intr_clr_nm,
        a.extr_clr_cd,
        a.extr_clr_nm,
        concat(a.extr_clr_cd, " - ", a.extr_clr_nm) as exteriorcolorid,
        a.vhcl_nmc_mdl_cd as NNA_SLS_MDL_CD,
        a.vhcl_nmc_mdl_nm   as NNA_SLS_MDL_NM,
        concat(a.vhcl_nmc_mdl_cd , " - ", a.vhcl_nmc_mdl_nm) as salescodeid,
        a.vhcl_engn_mdl_cd,
        a.fuel_systm_cd,
        a.orgnl_whlsl_dlr_nb,
        a.orgnl_rtl_dlr_nb as retail_dlrnb,
        a.orgnl_rtl_st_cd as retail_state,
        retail_dlr.dlr_pstl_cd as retail_zipcode,
        substr(retail_dlr.dlr_pstl_cd, 1,3) as retail_zip3,
        trim(upper(a.fctry_optns_tx)) as fctry_optns_tx,
        trim(lower(a.emsn_typ_nm)) as emsn_typ_nm,
        CASE WHEN isnull(altitude.zip3) THEN 0 ELSE 1 END as v_altitude,
        CASE WHEN isnull(cold.zip3) THEN 0 ELSE 1 END as v_cold,
        CASE WHEN isnull(dry.zip3) THEN 0 ELSE 1 END as v_dry,
        CASE WHEN isnull(hot.zip3) THEN 0 ELSE 1 END as v_hot,
        CASE WHEN isnull(humid.zip3) THEN 0 ELSE 1 END as v_humid,
        CASE WHEN isnull(rain.zip3) THEN 0 ELSE 1 END as v_rain,
        CASE WHEN isnull(salt.zip3) THEN 0 ELSE 1 END as v_salt,
        CASE WHEN isnull(solar.zip3) THEN 0 ELSE 1 END as v_solar, ',sold_flag , ',', v_mis_vec, sep='')

#--building vehicle filters----
v_condition <- 'a.nml_prdctn_mdl_cd <> "" and
                unix_timestamp(trim(a.mnfctg AND
                       TO_DATE(substr(a.actl_dlr_dlvry_dt,1,10),"MM/dd/yyyy") > TO_DATE("01/01/1900","MM/dd/yyyy")
                  THEN DATEDIFF(CURRENT_DATE(),
                       TO_DATE(substr(a.actl_dlr_dlvry_dt,1,10),"MM/dd/yyyy") )
                  ELSE NULL
               END ) as storage3 ', sep='')


#  Note: In service date >= manufacturing date
sold_condition= paste(' DATEDIFF(', adj_orgnl_INSVCDT,
                      ',TO_DATE(substr(a.mnfctg_dt,1,10), "MM/dd/yyyy")) >=0 ',sep="")


sold_flag<- paste(' CASE WHEN (', sold_condition, ') THEN "YES" ELSE "NO" END as sold_flag',sep='')

long_whlsl_flag<- ' ( CASE WHEN  DATEDIFF(TO_DATE(substr(a.orgnl_whlsl_dt, 1,10),"MM/dd/yyyy"), TO_DATE(substr(a.mnfctg_dt,1,10),"MM/dd/yyyy") ) > 180 and
                                 TO_DATE(substr(a.mnfctg_dt,1,10),"MM/dd/yyyy") > TO_DATE("01/01/1900","MM/dd/yyyy")  and
                                  a.orgnl_whlsl_dt is not null
                           THEN  "Y"
                           ELSE  "N"
                        END ) as long_whlsl_flag '

dlr_storage_days<-paste(' CASE WHEN (', sold_condition, ') THEN ', dlr_s01, ' ELSE ' , dlr_s02, ' END as dlr_storage_days ',sep='')

dlr_storage_months<-paste(' CASE WHEN (', sold_condition, ') THEN ', dlr_s03, ' ELSE ' , dlr_s04, ' END as dlr_storage_months ',sep='')

v_mis <- paste(' FLOOR(DATEDIFF(CURRENT_DATE(),', adj_orgnl_INSVCDT, ')/30.4) ', sep="")

v_mis_n <- paste(' DATEDIFF(CURRENT_DATE(),', adj_orgnl_INSVCDT, ')/30.4 ', sep="")


#---- v_mis 0-180---#
v_mis_seq<- 1:180
txt0<-paste(' CASE WHEN (', v_mis, '>=',v_mis_seq, ') THEN 1 ELSE 0 END as vmis',v_mis_seq,sep='',collapse= ',')
v_mis_vec<-paste(' CASE WHEN (', sold_condition, ') THEN 1 ELSE 0 END as vmis0, ', txt0, sep='')

v_fields <- paste(' CURRENT_DATE() as ReportDate,
        1 as TableFlag, a.ls_up_tm_stmp as vehicle_last_time_stamp,
        TO_DATE(substr(a.crte_tm_stmp, 1,10), "MM/dd/yyyy") as create_date,
        a.vin_id as vvin_id,
        substr(a.vhcl_yr_nb,1,4) as vhcl_yr_nb,
        a.vhcl_mk_cd as vhcl_make,
        a.nml_prdctn_mdl_cd,
        a.nml_prdctn_mdl_cd as vehprodcd,
        trim(upper(a.vhcl_ln_nm)) as  vhcl_ln_nm,
        a.glbl_mrkt_cd,
        a.asct_dstrbr_cd,
        a.fclty_dstrbr_cd,
        c.profile,
        c.mu,
        c.sigma,
        a.mnfctg_vhcl_plnt_cd,
        b.vhcl_mnfctr_plnt_nm as plant_name,
        substr(a.mnfctg_dt,1,10) as mnfctg_dt,
        concat(substr(a.mnfctg_dt,7,4),substr(a.mnfctg_dt,1,2)) as prodmth, ',
        adj_orgnl_INSVCMTH,' as insvcmth,
        a.rtl_sl_lse_dt, ',
        adj_orgnl_INSVCDT2,' as orgnl_in_svc_dt,
        substr(a.in_svc_dt,1,10) as in_svc_dt',
        ',',v_orgnl_in_svc_dt,
        ',',v_actl_dlr_dlvry_dt,
        ',',v_orgnl_whlsl_dt,
        ',',v_fctry_ivntry_trnsfr_dt,
        ',',v_mnfctg_dt,
        ',',storage1,
        ',',storage2,
        ',',storage3,
        ',',long_whlsl_flag,
         ',',dlr_storage_days,
        ',',dlr_storage_months,
        ',',v_mis,' as v_mis ',
        ',',v_mis_n,' as v_mis_n ',
        ', a.vhcl_flt_in,
        a.asgnd_load_in,
        a.cv_in,
        a.us_spcfcn_in,
        a.vhcl_blmt_shpmnt_in,
        a.vhcl_crshd_in,
        a.vhcl_frgmt_in,
        a.vhcl_hld_in,
        a.vhcl_rtrn_in,
        a.vhcl_flt_cntct_nm,
        a.vhcl_ownr_nm,
        a.engn_prfx_8_dgt_cd,
        a.vhcl_idfctn_4_dgt_nb,
        a.tlmtcs_svc_vndr_cd,
        a.navi_plnt_scan_ts,
        a.navi_id,
        a.sim_id,
        a.drv_trn_ds,
        a.btry_12_volt_srl_nb,
        a.trm_cd as intr_clr_cd,
        a.trm_cd_nm as intr_clr_nm,
        a.extr_clr_cd,
        a.extr_clr_nm,
        concat(a.extr_clr_cd, " - ", a.extr_clr_nm) as exteriorcolorid,
        a.vhcl_nmc_mdl_cd as NNA_SLS_MDL_CD,
        a.vhcl_nmc_mdl_nm   as NNA_SLS_MDL_NM,
        concat(a.vhcl_nmc_mdl_cd , " - ", a.vhcl_nmc_mdl_nm) as salescodeid,
        a.vhcl_engn_mdl_cd,
        a.fuel_systm_cd,
        a.orgnl_whlsl_dlr_nb,
        a.orgnl_rtl_dlr_nb as retail_dlrnb,
        a.orgnl_rtl_st_cd as retail_state,
        retail_dlr.dlr_pstl_cd as retail_zipcode,
        substr(retail_dlr.dlr_pstl_cd, 1,3) as retail_zip3,
        trim(upper(a.fctry_optns_tx)) as fctry_optns_tx,
        trim(lower(a.emsn_typ_nm)) as emsn_typ_nm,
        CASE WHEN isnull(altitude.zip3) THEN 0 ELSE 1 END as v_altitude,
        CASE WHEN isnull(cold.zip3) THEN 0 ELSE 1 END as v_cold,
        CASE WHEN isnull(dry.zip3) THEN 0 ELSE 1 END as v_dry,
        CASE WHEN isnull(hot.zip3) THEN 0 ELSE 1 END as v_hot,
        CASE WHEN isnull(humid.zip3) THEN 0 ELSE 1 END as v_humid,
        CASE WHEN isnull(rain.zip3) THEN 0 ELSE 1 END as v_rain,
        CASE WHEN isnull(salt.zip3) THEN 0 ELSE 1 END as v_salt,
        CASE WHEN isnull(solar.zip3) THEN 0 ELSE 1 END as v_solar, ',sold_flag , ',', v_mis_vec, sep='')

#--building vehicle filters----
v_condition <- 'a.nml_prdctn_mdl_cd <> "" and
                unix_timestamp(trim(a.mnfctg
