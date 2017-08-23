package AssetsPortraitNew

import java.io.{BufferedInputStream, FileInputStream}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.fpm.AssociationRules.Rule
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.elasticsearch.spark.sql._
import utils.ROWUtils

import scala.util.Try
import scala.sys.process._

/**
  * Created by bai on 2017/7/18.
  */
object FrequentItemset {

  //main函数
  def main(args: Array[String]): Unit = {

    //配置日志文件
    val filePath = System.getProperty("user.dir")
    PropertyConfigurator.configure(filePath + "/conf/log4jtyb.properties")

    //读取配置文件
    val properties: Properties = new Properties()
    val ipstream = new BufferedInputStream(new FileInputStream(filePath + "/conf/frequent.properties"))
    properties.load(ipstream)

    //制作spark
    val spark = getSparkSession(properties)

    //执行readyMain函数
    val ReadyMain = new ReadyMainClass(properties, spark)
    ReadyMain.readyMain()
  }

  //配置spark
  def getSparkSession(properties: Properties): SparkSession = {
    val masterUrl = properties.getProperty("spark.master.url")
    val appName = properties.getProperty("spark.app.name")
    val sparkconf = new SparkConf()
      .setMaster(masterUrl)
      .setAppName(appName)
      .set("spark.port.maxRetries", properties.getProperty("spark.port.maxRetries"))
      .set("spark.cores.max", properties.getProperty("spark.cores.max"))
      .set("spark.executor.memory", properties.getProperty("spark.executor.memory"))
      .set("spark.sql.shuffle.partitions", properties.getProperty("spark.sql.shuffle.partitions"))
      .set("es.nodes", properties.getProperty("es.nodes"))
      .set("es.port", properties.getProperty("es.port"))
    val Spark = SparkSession.builder().config(sparkconf).getOrCreate()
    Spark
  }
}

//用于存放计算后的结果数据
case class dataDFResult(row: String, ip: String, rule: String, recommend: String, support: Int, confidence: Float, cltime: Long) extends Serializable

/**
  * main函数之前的准备项
  */

//主函数的准备函数
class ReadyMainClass(properties: Properties, spark: SparkSession) extends LoggerSupport with Serializable {
  //准备函数的主函数
  def readyMain(): Unit = {

    //如果存在今天的结果数据，那么就删除它
    val deteledata = properties.getProperty("deteledata")
    logger.error("执行" + deteledata)
    Try(deteledata.!!).getOrElse(logger.error("No such file or directory"))

    //从hbase中获取数据储存到HDFS
    val MoniflowGetData = new MoniflowGetDataClass(properties, spark)
    MoniflowGetData.getDataMain()

    //读取HDFS获得所有数据并持久化
    val datapath = properties.getProperty("dataPath")
    val dataoptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> datapath)
    val dataall = spark.read.options(dataoptions).format("com.databricks.spark.csv").load()
    dataall.persist(StorageLevel.MEMORY_AND_DISK)
    dataall.show(10)

    //生成计算时间
    val CLTIME = new Date().getTime
    //配置读取IP项
    var IPS: List[String] = Nil
    val ips = properties.getProperty("ips")
    if (ips == "true") {
      IPS = getIPList(dataall).toList
    } else {
      IPS = ips.split(",").toList
      logger.error("需要计算的ip：" + IPS.mkString)
    }

    //对每个IP进行频繁项集计算并将结果储存到HDFS
    IPS.foreach {
      ip =>
        val saveHdfsPath = properties.getProperty("saveHdfsPath")
        val saveEsPath = properties.getProperty("saveEsPath")
        val resultDF = runmodel(ip, dataall, CLTIME)
        if (resultDF != null && resultDF.take(10).length != 0) {
          val saveOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> saveHdfsPath)
          resultDF.write.options(saveOptions).mode(SaveMode.Append).format("com.databricks.spark.csv").save()
          logger.error(ip + "写入HDFS成功" + saveHdfsPath)
          resultDF.saveToEs(saveEsPath)
          logger.error(ip + "写入ES成功" + saveEsPath)
        }
    }
  }

  //获取ip的种类
  def getIPList(dataall: DataFrame): Array[String] = {
    dataall.createOrReplaceTempView("BIGDATA")
    //获取ip的种类
    val SRCIP: Array[String] = spark.sql(s"SELECT DISTINCT SRCIP FROM BIGDATA").rdd.map {
      row => List(row.getAs[String]("SRCIP"))
    }.reduce((x, y) => x ++ y).toArray
    SRCIP
  }

  //运行模型的函数
  def runmodel(ip: String, dataall: DataFrame, CLTIME: Long): DataFrame = {
    val handledatastarttime = (new Date().getTime + "").substring(0, 10).toLong
    logger.error("ip：" + ip + "开始处理数据：" + handledatastarttime.toString)

    val dataDF: DataFrame = dataInit(ip, dataall)

    val handledataendtime = (new Date().getTime + "").substring(0, 10).toLong
    logger.error("ip：" + ip + "处理数据完毕：" + handledataendtime.toString + "，花费时间：" + (handledataendtime - handledatastarttime).toString)

    val modelstarttime = (new Date().getTime + "").substring(0, 10).toLong
    logger.error("ip：" + ip + "模型开始：" + modelstarttime.toString)

    //构造数据类型
    val arrayRdd: RDD[Array[String]] = dataDF.select("HOST").rdd.map(_ (0).toString.split(",").distinct.toArray)

    //调用模型函数
    val fpmodel = new FpModel()
    var resultDF: DataFrame = null

    val minSupport = properties.getProperty("minSupport")
    val minConfidence = properties.getProperty("minConfidence")

    if (arrayRdd != null) {
      if (arrayRdd.take(10).length != 0) {
        //模型计算结果
        resultDF = fpmodel.mainfpmodel(arrayRdd, ip, spark, minSupport.toFloat, minConfidence.toFloat, CLTIME)
      }
    }
    val modelendtime = (new Date().getTime + "").substring(0, 10).toLong
    logger.error("ip：" + ip + "模型完毕：" + modelendtime.toString + "，花费时间：" + (modelendtime - modelstarttime).toString)
    //显示结果
    resultDF
  }

  //获取24个时段的host
  def dataInit(ip: String, dataall: DataFrame): DataFrame = {
    val readdatastarttime = (new Date().getTime + "").substring(0, 10).toLong
    logger.error("开始读取数据：" + readdatastarttime.toString)

    val ResultDF = getDataOut(ip, dataall)

    val readdataendtime = (new Date().getTime + "").substring(0, 10).toLong
    logger.error("读取数据完毕：" + readdataendtime.toString + "，花费时间：" + (readdataendtime - readdatastarttime).toString)

    ResultDF.createOrReplaceTempView("BIGDATA")
    val dataDF = spark.sql("select IP,DATE,concat_ws(',',collect_set(HOST)) as HOST from BIGDATA group by DATE,IP")
    logger.error("数据处理展示：")
    dataDF
  }

  //读取并筛选数据
  def getDataOut(ip: String, dataall: DataFrame): DataFrame = {
    dataall.createOrReplaceTempView("BIGDATA")
    val stringsql: String = "REGEXP_EXTRACT(HOST, '((^[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}$)|([\\\\w]*?\\.[\\\\w]+?\\.[\\\\w]+?$))') AS HOST"
    val dataDF = spark.sql(s"SELECT SRCIP," + stringsql + ",DATE FROM BIGDATA WHERE SRCIP = '" + ip + "'" + s"AND APPPROTO='HTTP' AND HOST IS NOT NULL AND HOST <> ''")
      .toDF("SRCIP", "HOST", "DATE")
    import spark.implicits._
    val ResultDF = dataDF.rdd.map {
      row =>
        val HOST = row.getAs[String]("HOST")
        val IP = ip
        val DATE = fmtime(row.getAs[String]("DATE"))
        (IP, HOST, DATE)
    }.toDF("IP", "HOST", "DATE")
    ResultDF
  }

  //这里是处理数据库时间的函数
  def fmtime(timestring: String): String = {
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //将string的时间转换为date
    val time: Date = fm.parse(timestring)
    val cal = Calendar.getInstance()
    cal.setTime(time)
    //提取时间里面的小时时段
    val hour = cal.get(Calendar.HOUR_OF_DAY).toString
    hour
  }
}

/**
  * FPtree主函数
  */
class FpModel {
  //模型主函数
  def mainfpmodel(data: RDD[Array[String]], ip: String, spark: SparkSession, minSupport: Float, minConfidence: Float, CLTIME: Long): DataFrame = {
    import spark.implicits._
    //创建一个FPGrowth的算法实列
    val fpg = new FPGrowth()
    fpg.setMinSupport(minSupport)

    //把数据带入算法中
    val model = fpg.run(data)

    //通过置信度筛选出推荐规则则
    val ResuleRdd: RDD[Rule[String]] = model.generateAssociationRules(minConfidence)
    val freqArray = model.freqItemsets.collect()
    val tempDF = ResuleRdd.map {
      rule =>
        val ROW = ROWUtils.genaralROW()
        val RULE = rule.antecedent.mkString(",").replace(",", "#")
        val RECOMMEND = rule.consequent.mkString(",").replace(",", "#")
        val CONFIDENCE = rule.confidence.toString.toFloat
        var SUPPORT = 0
        freqArray.foreach {
          itemset => {
            val rules = itemset.items.mkString(",").replace(",", "#")
            if (RULE == rules) {
              SUPPORT = itemset.freq.toString.toInt
            }
          }
        }
        dataDFResult(ROW, ip, RULE, RECOMMEND, SUPPORT, CONFIDENCE, CLTIME)
    }.toDF()
    tempDF
  }
}

/**
  * 从hbase读取原始数据到HDFS
  */
class MoniflowGetDataClass(properties: Properties, spark: SparkSession) extends LoggerSupport with Serializable {

  //读取es的数据并储存到HDFS
  def getDataMain(): Unit = {
    //从es中获取到moniflow的数据
    val moniflowData = getSIEMDataFromEs(spark, properties)
    moniflowData.show(10,false)
    logger.error("开始写入HDFS")
    if (moniflowData != null && moniflowData.take(10).length > 0) {
      val dataPath = properties.getProperty("dataPath")
      val saveOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> dataPath)
      moniflowData.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
      logger.error("写入HDFS成功")
    }
  }

  //连接es获取数据
  def getSIEMDataFromEs(spark: SparkSession, properties: Properties): DataFrame = {
    logger.error("开始连接es获取数据")
    //es表名
    val moniflowIndex = properties.getProperty("moniflowIndex")
    val nowDay = properties.getProperty("nowDay").toInt

    //抓取时间范围
    val start = Timestamp.valueOf(getTime(nowDay - 31)).getTime
    val end = Timestamp.valueOf(getTime(nowDay - 0)).getTime
    val query = "{\"query\":{\"range\":{\"date\":{\"gte\":" + start + ",\"lte\":" + end + "}}}}"
    //抓取数据下来
    val rowDF = spark.esDF(moniflowIndex, query)
    //转换时间格式
    val timeTransUDF = org.apache.spark.sql.functions.udf(
      (time: Long) => {
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time)
      }
    )
    val moniDF = rowDF.withColumn("timeTrans", timeTransUDF(rowDF("date")))
    //转换大小写
    moniDF.createOrReplaceTempView("moni")
    val DataMN = spark.sql(
      """
        |SELECT id AS ID,timeTrans AS DATE,srcip AS SRCIP,dstip AS DSTIP,appproto AS APPPROTO,
        |srcport AS SRCPORT,dstport AS DSTPORT,host AS HOST
        |FROM moni where HOST is not null
      """.stripMargin)
    DataMN
  }

  //获取时间
  def getTime(day: Int): String = {
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, day)
    val time: Date = cal.getTime
    val newtime: String = new SimpleDateFormat("yyyy-MM-dd" + " 00:00:00").format(time)
    newtime
  }
}

/**
  * 日志系统防止序列化
  */
trait LoggerSupport {
  @transient lazy val logger = Logger.getLogger(this.getClass)
}
