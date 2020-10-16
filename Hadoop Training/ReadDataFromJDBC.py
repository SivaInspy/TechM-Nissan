config = new SparkConf().setAppName("Read JDBC Data")
# JDBC Connection Details
driver = "oracle.jdbc.driver.OracleDriver"
url = "jdbc:oracle:thin:@127.0.0.1:1521:ORCL"
user = "root"
pswd = "Ikshu@5417"
sourceTable = "employee"
# JDBC connection and load table in dataframe
empDF = spark.read \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", sourceTable) \
    .option("user", user) \
    .option("password", pswd) \
    .option("driver", driver) \
    .load()

# Read data from Dataframe
sourceDf.show()


2030401510970197
