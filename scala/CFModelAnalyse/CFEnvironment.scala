package CFModelAnalyse

import java.io.{BufferedInputStream, FileInputStream, InputStream}
import java.util.Properties
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by huoguang on 2017/5/5.
  */
class CFEnvironment {
  def getSparkSession(): SparkSession = {
    //读取配置文件
//    val properties: Properties = new Properties()
//    val ipstream: InputStream = this.getClass().getResourceAsStream("/config.properties")
//    properties.load(ipstream)
    val filePath = System.getProperty("user.dir")
    val properties: Properties = new Properties()
    val ipstream = new BufferedInputStream(new FileInputStream(filePath + "/conf/CFModel.properties"))
    properties.load(ipstream)

    val masterUrl = properties.getProperty("spark.master.url")
    val appName = properties.getProperty("spark.app.name")
    val sparkconf = new SparkConf()
      .setMaster(masterUrl)
      .setAppName(appName)
      .set("spark.port.maxRetries", properties.getProperty("spark.port.maxRetries"))
      .set("spark.cores.max",properties.getProperty("spark.cores.max"))
      .set("spark.executor.memory",properties.getProperty("spark.executor.memory"))
      .set("es.nodes",properties.getProperty("es.nodes"))
      .set("es.port",properties.getProperty("es.port"))
    val Spark = SparkSession.builder().config(sparkconf).getOrCreate()
    Spark
  }
}
