#########################################################################################################################################
#                                                                                                                                       #
#               Created by        : Nandha Kumaran M marimn1                                                                            #
#               Last Updated by   : Nandha Kumaran M marimn1                                                                            #
#               Last Updated date : 06/28/2018                                                                                          #
#               Script Name       : incident_rate_daily.sh                                                                              #
#               Description       : Load the IR tables daily                                                                            #
#               Log file path     : /projects/equip/logs                                                                                #
#########################################################################################################################################



# This fucntion is to load IR data using pyspark program

loadIRData()
{
        export SPARK_MAJOR_VERSION=2
        echo "Starting to execute $1 ..." | hadoop fs -appendToFile - $2
        hdfs dfs -get $3/$1
        echo "hdfs dfs -get $3/$1"
        #spark-submit --master yarn --deploy-mode client --num-executors 20 --executor-memory 2g --executor-cores 2  $1
        spark-submit --master yarn --deploy-mode client --num-executors 50 --executor-memory 6g --driver-memory 2g $1
        echo "Finished executing the script "$1" and inserted into the base table" | hadoop fs -appendToFile - $2
}

# Main script starts here. Uses 6 SPARK scripts to load the incident rate tables.

echo "*************************************************************************************************"  | hadoop fs -appendToFile - $2
date | hadoop fs -appendToFile - $2
echo "*************************************************************************************************" | hadoop fs -appendToFile - $2

loadIRData $1 $2 $3
