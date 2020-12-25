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



from pyspark.sql import *
Employee = Row("firstName", "lastName", "email", "salary")
employee1 = Employee('Basher', 'armbrust', 'bash@edureka.co', 100000)
employee2 = Employee('Daniel', 'meng', 'daniel@stanford.edu', 120000 )
employee3 = Employee('Muriel', None, 'muriel@waterloo.edu', 140000 )
employee4 = Employee('Rachel', 'wendell', 'rach_3@edureka.co', 160000 )
employee5 = Employee('Zach', 'galifianakis', 'zach_g@edureka.co', 160000 )
print(Employee[0])
print(employee3)
department1 = Row(id='123456', name='HR')
department2 = Row(id='789012', name='OPS')
department3 = Row(id='345678', name='FN')
department4 = Row(id='901234', name='DEV')
