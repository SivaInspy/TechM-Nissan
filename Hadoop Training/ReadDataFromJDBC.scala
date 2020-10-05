import java.sql.{Connection, ResultSet}
import java.sql.DriveManager
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object ReadDataFromJDBC{
  def main(args:Array[String]): Unit = {
    val sourceTable = args(0)
    //Spark Configuration Setup

    val config = new SparkConf().setAppName("Read JDBC Data: "+ sourceTable)
    config.set("spark.driver.allowMultipleContexts", "true")

    try{
      print("Started ...")

      // JDBC Connection Details
      val driver = "com.mysql.jdbc.Driver"
      val url = "jdbc:mysql://127.0.0.1:3306/mysql_siva"
      val user = "root"
      val pass = "Ikshu@5417"

      // JDBC connection and load table in dataframe
      val sourceDf = spark.read.format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("dbTable", sourceTable)
      .option("user", user)
      .option("password", pass)
      .load()

    // Read data from Dataframe
      sourceDf.show()
    }catch{
      case e : Throwable => ("Connectivity failed for Table", e)
    }
    }
  }
