/spark-2.1.0-bin-hadoop2.7/bin/
export SPARK_MAJOR_VERSION=2
export CLASSPATH=$PWD/ojdbc6.jar
export PYSPARK_SUBMIT_ARGS="--master local[2] pyspark-shell"
pyspark --jars "/home/siva_inspy/ojdbc6.jar" --deploy-mode cluster --num-executors 2 --driver-memory 2g --executor-memory 1g

empDF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:username/password@//hostname:portnumber/SID") \
    .option("dbtable", query) \
    .option("user", "db_user_name") \
    .option("password", "password") \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .load()


sudo rpm -Uhv sqldeveloper-19.2.1.247-1.noarch.rpm

rpm -Uhv sqldeveloper-(build number)-1.noarch.rpm

19.2.1.247.2212
