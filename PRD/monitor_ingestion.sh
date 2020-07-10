#!/bin/bash
Solr()
{
   #!/bin/bash
   kinit -kt /etc/security/keytabs/X981138.keytab x981138

   hdfs dfs -get hdfs:///projects/common/environmentVars.sh
   source ./environmentVars.sh
   rm environmentVars.sh  

   #Extracting one Solr hostname from the array of hosts. E.g. sample format
   #solr_endpoint="usnencpl084.nmcorp.nissan.biz:8983"
   IFS=','
   read -ra solr_endpoint <<< "$SOLRHOSTS"
   solr_endpoint="${solr_endpoint[0]}":8983
   curl_output=`curl --negotiate -u : "http://$solr_endpoint/solr/$objct_typ_nm/select?q=*%3A*&wt=json&&q=*:*&rows=0&indent=true"`
   solr_count=`echo $curl_output| grep "numFound" |cut -d':' -f3|cut -d ' ' -f1`
   echo "curl_output" $curl_output
   echo "curl" "curl --negotiate -u : http://$solr_endpoint/solr/$objct_typ_nm/select?q=*%3A*&wt=json&&q=*:*&rows=0&indent=true"


   #beeline -u ${connectionString} -e "insert into bde_stats.load_objct_dtl partition(LOAD_DT = $today_dt_wq, DB_NM = $db_nm_wq , OBJCT_TYP_CD = $objct_typ_cd_wq , OBJCT_TYP_NM = $objct_typ_nm_wq) select $objct_dsply_nm_wq, $objct_dmn_nm_wq, $solr_count, 'monitor_ingestion',cast(from_unixtime(unix_timestamp()) as timestamp),'monitor_ingestion',cast(from_unixtime(unix_timestamp()) as timestamp) from bde_stats.load_objct limit 1"
   hive --hiveconf mapreduce.job.credentials.binary=$HADOOP_TOKEN_FILE_LOCATION --hiveconf tez.credentials.path=$HADOOP_TOKEN_FILE_LOCATION -e "insert into bde_stats.load_objct_dtl partition(LOAD_DT = $today_dt_wq, DB_NM = $db_nm_wq , OBJCT_TYP_CD = $objct_typ_cd_wq , OBJCT_TYP_NM = $objct_typ_nm_wq) select $objct_dsply_nm_wq, $objct_dmn_nm_wq, $solr_count, 'monitor_ingestion',cast(from_unixtime(unix_timestamp()) as timestamp),'monitor_ingestion',cast(from_unixtime(unix_timestamp()) as timestamp) from bde_stats.load_objct limit 1"
   echo "insert into bde_stats.load_objct_dtl partition(LOAD_DT = $today_dt_wq, DB_NM = $db_nm_wq , OBJCT_TYP_CD = $objct_typ_cd_wq , OBJCT_TYP_NM = $objct_typ_nm_wq) select $objct_dsply_nm_wq, $objct_dmn_nm_wq, $solr_count, 'monitor_ingestion',cast(from_unixtime(unix_timestamp()) as timestamp),'monitor_ingestion',cast(from_unixtime(unix_timestamp()) as timestamp) from bde_stats.load_objct limit 1"
}


table()
{
   echo " executing table"
   echo "hive --hiveconf mapreduce.job.credentials.binary=$HADOOP_TOKEN_FILE_LOCATION --hiveconf tez.credentials.path=$HADOOP_TOKEN_FILE_LOCATION -e select from_unixtime(unix_timestamp(max(updt_ts),'yyyy-MM-dd HH:mm:ss.SSS'),'yyyy-MM-dd HH:mm:ss') from bde_stats.load_objct_dtl where db_nm=$db_nm_wq and objct_typ_nm=$objct_typ_nm_wq and objct_typ_cd=$objct_typ_cd_wq"

   max_ts=`hive --hiveconf mapreduce.job.credentials.binary=$HADOOP_TOKEN_FILE_LOCATION --hiveconf tez.credentials.path=$HADOOP_TOKEN_FILE_LOCATION -e "select from_unixtime(unix_timestamp(max(updt_ts),'yyyy-MM-dd HH:mm:ss.SSS'),'yyyy-MM-dd HH:mm:ss') from bde_stats.load_objct_dtl where db_nm=$db_nm_wq and objct_typ_nm=$objct_typ_nm_wq and objct_typ_cd=$objct_typ_cd_wq"`
   rc=$?
   echo "max_ts" $max_ts
   echo "return1 = $rc"
   if [ $rc -ne 0 ] ;then exit 1; fi

   max_ts_wq="'${max_ts}'"
   if [ "$max_ts_wq" = "'NULL'" ]
   then
		   filter="'yyyy-MM-dd') >= ${ten_dt_bfr_wq}"
				   echo "spark-submit --master yarn --deploy-mode cluster --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./envs/bin/python --archives envs/envs.zip#envs --keytab ${keytabName} --principal ${principal} ${extraParams} ${pyScript} \"${db_nm}\" \"${objct_typ_nm}\" \"${objct_typ_cd_wq}\" \"${objct_dsply_nm_wq}\" \"${objct_dmn_nm_wq}\" \"${incmtl_clmn_nm}\" \"${incmtl_clmn_frmt_nm_wq}\" \"${filter}\""
				   spark-submit --master yarn --deploy-mode cluster --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./envs/bin/python --archives envs/envs.zip#envs --keytab ${keytabName} --principal ${principal} ${extraParams} ${pyScript} "${db_nm}" "${objct_typ_nm}" "${objct_typ_cd_wq}" "${objct_dsply_nm_wq}" "${objct_dmn_nm_wq}" "${incmtl_clmn_nm}" "${incmtl_clmn_frmt_nm_wq}" "${filter}"  &
		   rc=$?
				   echo "return2 = $rc"
		   if [ $rc -ne 0 ];then exit 1; fi
				   sleep 5
				else
								filter="'yyyy-MM-dd HH:mm:ss') > cast($max_ts_wq as timestamp)"
						echo "spark-submit --master yarn --deploy-mode cluster --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./envs/bin/python --archives envs/envs.zip#envs --keytab ${keytabName} --principal ${principal} ${extraParams} ${pyScript} \"${db_nm}\" \"${objct_typ_nm}\" \"${objct_typ_cd_wq}\" \"${objct_dsply_nm_wq}\" \"${objct_dmn_nm_wq}\" \"${incmtl_clmn_nm}\" \"${incmtl_clmn_frmt_nm_wq}\" \"${filter}\""
						spark-submit --master yarn --deploy-mode cluster --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./envs/bin/python --archives envs/envs.zip#envs --keytab ${keytabName} --principal ${principal} ${extraParams} ${pyScript} "${db_nm}" "${objct_typ_nm}" "${objct_typ_cd_wq}" "${objct_dsply_nm_wq}" "${objct_dmn_nm_wq}" "${incmtl_clmn_nm}" "${incmtl_clmn_frmt_nm_wq}" "${filter}"  &
						rc=$?
						echo "return3 = $rc"
					if [ $rc -ne 0 ];then exit 1; fi
								sleep 5
						fi

   spark_job=`ps -ef | grep -i $$ | grep -i "monitor_ingestion.py" | tr -s " " | cut -f8 -d " " | grep -i "bash" | wc -w`
   spark_job_wb=`ps -ef | grep -i $$ | grep -i "monitor_ingestion.py" | grep "/usr/java/" | tr -s " " | cut -f8 -d " " | grep -v "bash" | wc -w`
   ps -ef | grep -i $$ | grep -i "monitor_ingestion.py" | tr -s " " | grep -i "bash"
   echo "********"
   ps -ef | grep -i $$ | grep -i "monitor_ingestion.py" | tr -s " " | grep -v "bash"
		echo " $spark_job $spark_job_wb"
				while [ $spark_job -ge $noOfProcess -o $spark_job_wb -ge $noOfProcess ]
				do
				  echo "more than $noOfProcess spark jobs are running. Check after 5 seconds"
				  sleep 5
				  spark_job=`ps -ef | grep -i $$ | grep -i "monitor_ingestion.py" | tr -s " " | cut -f8 -d " " | grep -i "bash" | wc -w`

				  spark_job_wb=`ps -ef | grep -i $$ | grep -i "monitor_ingestion.py" | grep "/usr/java/" | tr -s " " | cut -f8 -d " " | grep -v "bash" | wc -w`
				  ps -ef | grep -i $$ | grep -i "monitor_ingestion.py" | tr -s " " | grep -i "bash"
				  echo "********"
				  ps -ef | grep -i $$ | grep -i "monitor_ingestion.py" | grep "/usr/java/" | tr -s " " | grep -v "bash"

				done

}

keytab=$1
principal=$2
connectionString=$3
hdfs dfs -get $keytab
keytabName=$principal".keytab"
kinit -kt $keytabName $principal
klist
noOfProcess=$4
pyScript=$5
extraParams=$6
objcttype=$7
objctdb=$8



loggingScriptPath=hdfs:///projects/common
environmentPath=$loggingScriptPath/envs.zip
rm -r envs
mkdir envs
hdfs dfs -get $environmentPath envs/envs.zip
pushd envs
unzip envs.zip
popd

export SPARK_MAJOR_VERSION=2
export PYTHONPATH=`pwd`
export PYSPARK_PYTHON=./envs/bin/python

echo "running Beeline1"
echo "Py" $pyScript
echo "select concat_ws('|',objct_typ_cd,objct_typ_nm,db_nm,objct_dsply_nm,objct_dmn_nm,incmtl_clmn_nm,incmtl_clmn_frmt_nm) from bde_stats.load_objct where objct_typ_cd in (${objcttype}) and db_nm in (${objctdb})"

beeline -u ${connectionString} --outputformat=csv2 -e "select concat_ws('|',objct_typ_cd,objct_typ_nm,db_nm,objct_dsply_nm,objct_dmn_nm,incmtl_clmn_nm,incmtl_clmn_frmt_nm) from bde_stats.load_objct where objct_typ_cd in (${objcttype}) and db_nm in (${objctdb})" > query_result.txt;
rc=$?
if [ $rc -ne 0 ] ;then exit 1; fi

echo "beeline -u ${connectionString} --outputformat=csv2 -e \"select concat_ws('|',objct_typ_cd,objct_typ_nm,db_nm,objct_dsply_nm,objct_dmn_nm,incmtl_clmn_nm,incmtl_clmn_frmt_nm) from bde_stats.load_objct  where objct_typ_cd in (${objcttype}) and db_nm in (${objctdb}\" > query_result.txt;"
ls -l query_result.txt
cat query_result.txt

sed '1d' query_result.txt > query_result_filtered.txt

while read -r line; do
   objct_typ_cd=`echo $line | cut -f1 -d "|" | tr --delete '\n'`
   objct_typ_cd_wq="'${objct_typ_cd}'"
   objct_typ_nm=`echo $line | cut -f2 -d "|" | tr --delete '\n'`
   objct_typ_nm_wq="'${objct_typ_nm}'"
   db_nm=`echo $line | cut -f3 -d "|" | tr --delete '\n'`
   db_nm_wq="'${db_nm}'"
   objct_dsply_nm=`echo $line | cut -f4 -d "|" | tr --delete '\n'`
   objct_dsply_nm_wq="'${objct_dsply_nm}'"
   objct_dmn_nm=`echo $line | cut -f5 -d "|" | tr --delete '\n'`
   objct_dmn_nm_wq="'${objct_dmn_nm}'"
   incmtl_clmn_nm=`echo $line | cut -f6 -d "|" | tr --delete '\n'`
   incmtl_clmn_nm_wq="'${incmtl_clmn_nm}'"
   incmtl_clmn_frmt_nm=`echo $line | cut -f7 -d "|" | tr --delete '\n'`
   incmtl_clmn_frmt_nm_wq="'${incmtl_clmn_frmt_nm}'"
   today_dt=`date +%Y-%m-%d`
   today_dt_wq="'${today_dt}'"
   ten_dt_bfr=`date +%Y-%m-%d -d "100 day ago" | tr --delete '\n'`
   ten_dt_bfr_wq="'${ten_dt_bfr}'"

   if [ "$objct_typ_cd_wq" == "'table'" ];
   then
		   table
   else
		   Solr
   fi

done < query_result_filtered.txt

spark_job_wb=`ps -ef | grep -i $$ | grep -i "monitor_ingestion.py" | tr -s " " | cut -f8 -d " " | grep -v "bash" | wc -w`

while [ $spark_job_wb -ne 0 ]
do
	sleep 5
	spark_job_wb=`ps -ef | grep -i $$ | grep -i "monitor_ingestion.py" | tr -s " " | cut -f8 -d " " | grep -v "bash" | wc -w`

done
hive --hiveconf mapreduce.job.credentials.binary=$HADOOP_TOKEN_FILE_LOCATION --hiveconf tez.credentials.path=$HADOOP_TOKEN_FILE_LOCATION -e "ALTER TABLE bde_stats.load_objct_dtl DROP PARTITION (load_dt  <= $ten_dt_bfr_wq)"
rc=$?
if [ $rc -ne 0 ];then exit 1; fi
echo "beeline -u ${connectionString} --showHeader=false --outputformat=tsv2 -e ALTER TABLE bde_stats.load_objct_dtl DROP PARTITION (load_dt  <= $ten_dt_bfr_wq)"
rm $keytabName
exit 0

