#!/bin/bash
Date_,Open,High,Low,Close,Volume,Name

# MySql
create table mysql_hive.stocks3 (Date_ DATE, Open DOUBLE(16,4) NOT NULL, High DOUBLE(16,4) NOT NULL, Low DOUBLE(16,4) NOT NULL, Close DOUBLE(16,4) NOT NULL, Volume_for_the_day INT NOT NULL, Name VARCHAR(100) NOT NULL);
LOAD DATA LOCAL INFILE '/home/siva_inspy/AAL_Data4.csv' INTO TABLE mysql_hive.stocks3 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;


# Hive
create table stocks3 (Date_ String, Open Double, High Double, Low Double, Close Double, Volume_for_the_day int, Name String) row format delimited fields terminated by ',';
load data local inpath '/home/siva_inspy/AAL_Data4.csv' into table stocks;

#Partitioning
create table stocks_part(Date_ String, Open Double, High Double, Low Double, Close Double, Volume_for_the_day int) PARTITIONED BY(Name String);
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE stocks_part PARTITION(Name) SELECT * from  stocks3;

#Bucketing
set hive.enforce.bucketing=true;
create table stocks_buck(Date_ String, Open Double, High Double, Low Double, Close Double, Volume_for_the_day int, Name String) CLUSTERED BY(Name) into 4 buckets row format delimited fields terminated by ',';

# Views:
Create VIEW stocks_view AS SELECT * FROM stocks3 WHERE Open>40;

# Index:
create INDEX stocks_Index2 ON TABLE stocks3(Open) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD;

select Name,Date_,Close,lag(Close,1) over(partition by Name) as yesterday_price from stocks limit 10;
select Name,Date_,Close,case(lead(Close,1) over(partition by Name)-Close)>0 when true then "Higher" when false then "lesser" end as Changes from stocks order by Date_ limit 10;
select Name,first_value(High) over(partition by Name) as first_High from stocks;
select Name,last_value(High) over(partition by Name) as first_High from stocks;
select Name,count(Name) over(partition by Name) as cnt from stocks;
select Name,sum(Close) over(partition by Name) as total from stocks;
select Name,Date_,Volume_for_the_day,sum(Volume_for_the_day) over(partition by Name order by Date_) as running_total from stocks;
select Name,Date_,Volume_for_the_day,(Volume_for_the_day*100/(sum(Volume_for_the_day) over(partition by Name))) from stocks;
select Name, min(Close) over(partition by Name) as minimum from stocks;
select Name, max(Close) over(partition by Name) as maximum from stocks;
select Name, avg(Close) over(partition by Name) as maximum from stocks;
select Name,Close,rank() over(partition by Name order by Close) as closing from stocks;
select Name,Close,row_number() over(partition by Name order by Close) as num from stocks;
select Name,Close,dense_rank() over(partition by Name order by Close) as closing from stocks;
select Name,cume_dist() over(partition by Name order by Close) as cummulative from stocks;
select Name,Close,percent_rank() over(partition by Name order by Close) as closing from stocks;
select Name,ntile(3) over(partition by Name order by Close ) as bucket from stocks;


https://drive.google.com/file/d/18vtK3ofb-SF80o5exsHXU-J5f6sUcBc4/export?format=csv
https://drive.google.com/file/d/18vtK3ofb-SF80o5exsHXU-J5f6sUcBc4/export?format=csv
https://www.dropbox.com/s/gptueb9391blw63/AAL_data.csv?dl=1

hdfs://sandbox-hdp.hortonworks.com:8020/apps/hive/warehouse/stocks3

DELETE FROM stocks WHERE High IN (SELECT High FROM stocks WHERE Name='AAL') ;

select str_to_map("a=1 b=42 x=abc", " ", "=")["a"] as test;
select concat_ws(":1,",split("ABC",""));

sqoop import --connect jdbc:mysql://127.0.0.1:2222/mysql_hive --username root  --password Ikshu@5417 --table stocks3 --m 1
sqoop import --connect jdbc:mysql://127.0.0.1:3306/mysql_hive --username root  --password Ikshu@5417 --table stocks3 --m 1
sqoop import --connect jdbc:mysql://sandbox.hortonworks.com:2222/mysql_hive --username root  --password Ikshu@5417 --table stocks3 --m 1
sqoop import --connect jdbc:mysql://sandbox.hortonworks.com:3306/mysql_hive --username root  --password Ikshu@5417 --table stocks3 --m 1


sqoop create-hive-table --connect jdbc:mysql://127.0.0.1:2222/mysql_hive --table stocks3 --hive-table stock3 --username root --password Ikshu@5417

sqoop list-tables --connect jdbc:mysql://0.0.0.0:2222/mysql_hive --username root --password Ikshu@5417


sqoop import --connect jdbc:mysql://127.0.0.1:2222/mysql_hive
--username root
--password Ikshu@5417
--split-by id
--columns Volume_for_the_day,Name
--table stock3
--target-dir /home/siva_inspy/
--fields-terminated-by ","
--hive-import
--create-hive-table
--hive-table mysql_hive.stocks4

##################################################################################
systemctl stop mysqld
systemctl set-environment MYSQLD_OPTS="--skip-grant-tables --skip-networking"
systemctl start mysqld

mysql -uroot

Once in the mysql, run:
FLUSH PRIVILEGES;
UPDATE PASSWORD FOR root@'localhost'=PASSWORD('hadoop');
FLUSH PRIVILEGES;
QUIT;

Once log out from mysql, clear the environment MYSQLD_OPTS and restart mysql server

systemctl unset-environment MYSQLD_OPTS
systemctl restart mysqld
mysql -uroot -p
###################################################################################

#Java Home directory configuration
export JAVA_HOME="/etc/alternatives/java_sdk"
export PATH="$PATH:$JAVA_HOME/bin"

# Hadoop home directory configuration
export HADOOP_HOME=/usr/hdp/2.6.5.0-292/hadoop
export PATH=$PATH:$HADOOP_HOME/bin
export PATH=$PATH:$HADOOP_HOME/sbin

export HIVE_HOME=/usr/hdp/2.6.5.0-292/hive
export PATH=$PATH:$HIVE_HOME/bin
