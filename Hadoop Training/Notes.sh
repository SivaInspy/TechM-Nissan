/spark-2.1.0-bin-hadoop2.7/bin/
export SPARK_MAJOR_VERSION=2
export CLASSPATH=$PWD/ojdbc6.jar
export PYSPARK_SUBMIT_ARGS="--master local[2] pyspark-shell"
pyspark --jars "/home/siva_inspy/ojdbc6.jar" --deploy-mode cluster --num-executors 2 --driver-memory 2g --executor-memory 1g



sudo rpm -Uhv sqldeveloper-19.2.1.247-1.noarch.rpm

rpm -Uhv sqldeveloper-(build number)-1.noarch.rpm

19.2.1.247.2212
