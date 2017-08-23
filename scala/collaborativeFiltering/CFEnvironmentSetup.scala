package collaborativeFiltering

import java.io.InputStream
import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2017/4/25 0025.
  */
object CFEnvironmentSetup {
  def getSparkSession(): SparkSession = {
    //读取配置文件
    val properties: Properties = new Properties()
    val ipstream: InputStream = this.getClass().getResourceAsStream("/config.properties")
    properties.load(ipstream)

    val masterUrl = properties.getProperty("spark.master.url")
    val appName = properties.getProperty("spark.app.name")
    val Spark = SparkSession
      .builder()
      .master(masterUrl)
      .appName(appName)
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    Spark
  }
  def getLocalSparkSession(): SparkSession = {
    //构造sparksession的实例
    //读取配置文件
    val properties: Properties = new Properties()
    val ipstream: InputStream = this.getClass().getResourceAsStream("/config.properties")
    properties.load(ipstream)

    val masterUrl = "local[8]"
    val appName = properties.getProperty("spark.app.name")
    val Spark = SparkSession
      .builder()
      .master(masterUrl)
      .appName(appName)
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    Spark
  }

}
