#!/bin/bash

cd ../hive
hive -f data_extrcn_rule_bfr_run.hql

kinit -kt /etc/security/keytabs/X981138.keytab x981138@NMCORP.NISSAN.BIZ
hdfs dfs -cp /projects/equip/lib/.bidwPassword /projects/ews/lib/

# copying prerequisites for running the workflow
kinit -kt /etc/security/keytabs/x987731.keytab x987731@NMCORP.NISSAN.BIZ
# hdfs dfs -rm /data/lgl_iir/raw/incdnt_ivstgn_raw/* /data/lgl_iir/raw/incdnt_ivstgn_trd_raw/*
# hdfs dfs -mkdir -p /projects/ews/scripts/spark/search/conf/iir_sqoop
# hdfs dfs -mv -f iir_sqoop.properties /projects/ews/scripts/spark/search/conf/iir_sqoop/
# hdfs dfs -mv -f EWS_IIR_Ingest_Inc_daily.ini /projects/ews/scripts/spark/search/conf/
#running the lgl_iir automation workflow
oozie job --oozie http://usnencpl077.nmcorp.nissan.biz:11000/oozie --config sqoop_arch.properties -run

cd ../hive
hive -f data_extrcn_rule_aftr_run.hql
