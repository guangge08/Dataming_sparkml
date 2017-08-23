package CFAnalyse

import java.io.{BufferedInputStream, FileInputStream}
import java.util.{Date, Properties}

import CFModelAnalyse._
import CFModelAnalyse.CFEnvironment
import CFModelAnalyse.CFDataCleanProcess
import CFModelAnalyse.moniflowCluster
import CFModelAnalyse.CFModel
import CFModelAnalyse.netflowCluster
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}


/**
  * Created by huoguang on 2017/4/24.
  */
object CFMain {

  def main(args: Array[String]) {

    //配置<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    //配置spark集群
    Logger.getLogger("org").setLevel(Level.WARN) //+close info
    val sparkEnvironment = new CFEnvironment()
    val spark = sparkEnvironment.getSparkSession()
    val sc: SparkContext = spark.sparkContext
    val sql: SQLContext = spark.sqlContext
    //读取配置文件
    val filePath = System.getProperty("user.dir")
    val properties: Properties = new Properties()
    val ipstream = new BufferedInputStream(new FileInputStream(filePath + "/conf/CFModel.properties"))
    properties.load(ipstream)
    //获取数据储存路径(依次为：原始数据存放目录,统计后结果储存目录,协同过滤结果储存目录[推荐,user相似矩阵,product相似矩阵],聚类结果存放目录)

    //模型算法
    val time = new Date().getTime
    //获取数据<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
//    val CFDataCleanProcess = new CFDataCleanProcess()
//    val moniflow: (DataFrame, DataFrame) = CFDataCleanProcess.cleanData(sql,spark, properties,time, limitOption = false)

    //聚类<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    //规则分组
    val netflowClusterSec = new netflowClusterFour()
    netflowClusterSec.clustermain(time,properties,spark,sql)
//    //使用moniflow的数据
//    val moniflowCluster = new moniflowCluster()
//    moniflowCluster.cluster(moniflow,properties,spark)
//    //使用netflow的数据
//    val netflowCluster = new netflowCluster()
//    netflowCluster.clusterMain(properties)

    //协同过滤<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
//    val CFModel = new CFModel()
//    CFModel.CFModel(moniflow,time,properties,spark, sql)

    sc.stop()
    spark.stop()
  }

}
