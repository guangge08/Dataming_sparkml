package AssetsPortrait

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties, UUID}

import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import utils.ROWUtils

import scala.util.Try
import utils.HDFSUtils

import scala.sys.process._
import java.io.{BufferedInputStream, File, FileInputStream}

/**
  * Created by bai on 2017/6/30.
  */
object AssersPortraitSingleNew {

  def main(args: Array[String]): Unit = {

    //配置日志文件
    val filePath = System.getProperty("user.dir")
    PropertyConfigurator.configure(filePath + "/conf/log4jtyb.properties")

    //读取配置文件
    val properties: Properties = new Properties()
    val ipstream = new BufferedInputStream(new FileInputStream(filePath + "/conf/configkmeans.properties"))
    properties.load(ipstream)

    //获取spark
    val spark = getSparkSession(properties)
    //执行main函数
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
      .set("spark.sql.crossJoin.enabled", "true")
    val Spark = SparkSession.builder().config(sparkconf).getOrCreate()
    Spark
  }

}

//主函数的准备函数
class ReadyMainClass(properties: Properties, spark: SparkSession) extends LoggerSupport with HDFSUtils {
  //调用函数计算
  def readyMain() {

    //删除持久的基线结果
    val detelebaseline = properties.getProperty("detelebaseline")
    logger.error("执行" + detelebaseline)
    detelebaseline.!!

    val IPS = List("221.176.64.146", "221.176.64.166")
    val dataAll = getOriginalData(properties: Properties, spark: SparkSession)

    //筛选规则
    val JudgeRule = new JudgeRuleClass(properties, spark)

    //生成计算时间
    val CLTIME = getNowTime()
    val judgeIp = getIPList(dataAll, spark)
    for (ip <- IPS) {
      if (judgeIp.contains(ip)) {
        logger.error("计算ip" + ip + "开始")
        val CLNO = UUID.randomUUID().toString
        //筛选出内外网、异常端口、开放端口、正常端口
        val portData = JudgeRule.getNwAbportData(ip, dataAll, CLNO, CLTIME)
        val baseLineData = JudgeRule.getBaseLine(ip, CLNO, CLTIME, portData, spark)

        logger.error("开始储存结果")
        //保存持久的基线结果
        val baseline = properties.getProperty("baseline")
        val savebaselineOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> baseline)
        baseLineData.write.format("com.databricks.spark.csv").mode(SaveMode.Append).options(savebaselineOptions).save()

        //保存基线结果
        val BaseLineSave = properties.getProperty("BaseLineSave")
        val saveBaseLineOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> BaseLineSave)
        baseLineData.write.format("com.databricks.spark.csv").mode(SaveMode.Append).options(saveBaseLineOptions).save()
      }
    }
    logger.error("开始get到本地")
    val baseurl = properties.getProperty("hdfs.base.path")

    val srcpath_BaseLine = baseurl + properties.getProperty("resultdata.path.BaseLine")
    val dstpath_BaseLine = properties.getProperty("file.dst.path.BaseLine") + "T_SIEM_KMEANS_CLUSTER@" + getTime(-1) + "@BASELINE.csv"
    merge(srcpath_BaseLine, dstpath_BaseLine)

    //生成标识文件
    val flagname_BaseLine = "T_SIEM_KMEANS_CLUSTER@" + getTime(-1) + "@BASELINE.flag"
    val fileident_BaseLine = new File(properties.getProperty("file.dst.path.BaseLine") + flagname_BaseLine).createNewFile()

    //删除HDFS
    val deteleBaseLine = properties.getProperty("deteleBaseLine")
    logger.error("执行" + deteleBaseLine)
    deteleBaseLine.!!
  }

  //获取ip的种类
  def getIPList(dataFrame: DataFrame, spark: SparkSession): Array[String] = {
    logger.error("获取ip种类开始")
    dataFrame.createOrReplaceTempView("NETFLOWDATA")
    val SRCIPS: Array[String] = spark.sql(s"SELECT DISTINCT DSTIP FROM NETFLOWDATA WHERE FLOWD='1' OR FLOWD='2'").rdd.map(_.toSeq.toList.head.toString).collect()
    SRCIPS
  }

  //读取原始数据并持久化
  def getOriginalData(properties: Properties, spark: SparkSession): DataFrame = {
    logger.error("获取原始数据")
    val dataPath = properties.getProperty("Netflowpath")
    val dataList: List[DataFrame] = (-7 to -1).map { each =>
      val path = dataPath + getTime(each) + "--" + getTime(each + 1)
      logger.error(path)
      var result: DataFrame = null
      try {
        val dataOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> path)
        result = spark.read.options(dataOptions).format("com.databricks.spark.csv").load()
      } catch {
        case e: Exception => logger.error("出错：" + e.getMessage)
      }
      result
    }.toList.filter(_ != null)
    val JudgeRule = new JudgeRuleClass(properties, spark)
    var dataAll: DataFrame = null

    if (dataList.length > 1) {
      dataAll = JudgeRule.mergeDataFrame(dataList)
      dataAll = mergeData(dataAll, spark)
    } else {
      dataAll = dataList.head
      dataAll = mergeData(dataAll, spark)
    }
    dataAll.persist(StorageLevel.MEMORY_AND_DISK)
    dataAll
  }

  //对读取出来的数据进行合并处理
  def mergeData(dataFrame: DataFrame, spark: SparkSession): DataFrame = {

    //生成两个表查做做左关联
    dataFrame.createOrReplaceTempView("DATALEFT")
    dataFrame.createOrReplaceTempView("DATARIGHT")
    //join筛选条件的sql语句
    val joinSql = "DATALEFT.RECORDTIME=DATARIGHT.RECORDTIME and DATALEFT.SRCIP=DATARIGHT.DSTIP and DATALEFT.DSTIP=DATARIGHT.SRCIP and DATALEFT.SRCPORT=DATARIGHT.DSTPORT and DATALEFT.DSTPORT=DATARIGHT.SRCPORT"
    //选择字段的sql语句
    val selectSql = "DATALEFT.*,DATARIGHT.COSTTIME as costtimes,DATARIGHT.PACKERNUM as packnums,DATARIGHT.BYTESIZE as bytesizes,DATARIGHT.SRCPORT as srcports,DATARIGHT.DSTPORT as dstports"
    //执行sql
    val strSql = "select " + selectSql + " from DATALEFT left join DATARIGHT on " + joinSql
    var sqlData = spark.sql(strSql)

    //将时间差、包数量、字节大小相加
    //udf[String, Int][需要转换的类型,原来的类型]
    //查看类型println(sqlData.printSchema)
    val toStringUdf = udf[String, Int](_.toInt.toString)
    sqlData = sqlData.withColumn("sumcos", toStringUdf(sqlData("COSTTIME") + sqlData("costtimes")))
    sqlData = sqlData.withColumn("sumpac", toStringUdf(sqlData("PACKERNUM") + sqlData("packnums")))
    sqlData = sqlData.withColumn("sumbyt", toStringUdf(sqlData("BYTESIZE") + sqlData("bytesizes")))
    //比较源端口目的端口的大小
    val coder = (arg1: String, arg2: String) => {
      Try(if (arg1.toInt > arg2.toInt) 1 else 0).getOrElse(0)
    }
    val compareUdf = udf(coder)
    sqlData = sqlData.withColumn("compare", compareUdf(sqlData("SRCPORT"), sqlData("DSTPORT")))

    //获取结果的dataframe
    sqlData.createOrReplaceTempView("SQLDATA")

    //case when的sql
    val fieldsSql = "case when sumcos is not null then sumcos else COSTTIME end COSTTIME, TCP, UDP, ICMP,GRE, PROTO, case when sumpac is not null then sumpac else PACKERNUM end PACKERNUM, case when sumbyt is not null then sumbyt else BYTESIZE end BYTESIZE, FLOWD, ROW, RECORDTIME, SRCIP, DSTIP, SRCPORT, DSTPORT"
    val stringSql = "select " + fieldsSql + " from SQLDATA where srcports is null and compare=1 or srcports is not null and compare=1"
    val resultData = spark.sql(stringSql)
    resultData
  }


  //获取日期
  def getTime(day: Int): String = {
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, day)
    val time: Date = cal.getTime
    val newtime: String = new SimpleDateFormat("yyyy-MM-dd").format(time)
    newtime
  }

  //生成计算时间
  def getNowTime(): String = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time = new Date().getTime
    format.format(time)
  }
}

//判断规则的主类
class JudgeRuleClass(properties: Properties, spark: SparkSession) extends Serializable with LoggerSupport {
  //筛选出内外网、异常端口、开放端口、正常端口
  def getNwAbportData(ip: String, dataFrame: DataFrame, CLNO: String, TIME: String): DataFrame = {

    logger.error("筛选出内外网、异常端口、开放端口、正常端口开始")
    //筛选某个ip下的数据
    dataFrame.createOrReplaceTempView("NETFLOWDATA")
    val dataDF = spark.sql(s"SELECT COSTTIME,TCP,UDP,ICMP,GRE,PACKERNUM,BYTESIZE,PROTO,FLOWD,ROW,RECORDTIME,SRCIP,DSTIP,SRCPORT,DSTPORT FROM NETFLOWDATA WHERE DSTIP = '" + ip + "' OR SRCIP = '" + ip + "'")
      .toDF("COSTTIME", "TCP", "UDP", "ICMP", "GRE", "PACKERNUM", "BYTESIZE", "PROTO", "FLOWD", "ROW", "RECORDTIME", "SRCIP", "DSTIP", "SRCPORT", "DSTPORT")

    //筛选出有可能异常的端口
    dataDF.createOrReplaceTempView("PORTDATA")
    val abportData = spark.sql(s"SELECT DSTPORT FROM (SELECT DSTPORT,COUNT(DSTPORT) AS COUNTS FROM PORTDATA WHERE DSTIP = '" + ip + "' GROUP BY DSTPORT) AS T WHERE COUNTS < 30 ")
    val abportList = abportData.rdd.map(_.toSeq.toList.head.toString).collect().toList

    import spark.implicits._
    val ResultDF = dataDF.rdd.map {
      row =>
        val recordid: String = ROWUtils.genaralROW()
        val clno: String = CLNO.replace("-", "")
        val cltime: String = TIME
        val name = ip
        val dataori: String = "s"

        val COSTTIME = row.getAs[String]("COSTTIME")
        val TCP = row.getAs[String]("TCP")
        val UDP = row.getAs[String]("UDP")
        val ICMP = row.getAs[String]("ICMP")
        val GRE = row.getAs[String]("GRE")
        val PACKERNUM = row.getAs[String]("PACKERNUM")
        val BYTESIZE = row.getAs[String]("BYTESIZE")
        val PROTO = row.getAs[String]("PROTO")
        val FLOWD = row.getAs[String]("FLOWD")
        val ROW = row.getAs[String]("ROW")
        val RECORDTIME = row.getAs[String]("RECORDTIME")
        val SRCIP = row.getAs[String]("SRCIP")
        val DSTIP = row.getAs[String]("DSTIP")
        val SRCPORT = row.getAs[String]("SRCPORT")
        val DSTPORT = row.getAs[String]("DSTPORT")
        val DATE = fmtime(RECORDTIME)

        var PORT = ""
        var ABPORT = ""
        var INDEX = ""
        //ABPORT=0非异常数据
        //ABPORT=1异常端口且也是开放端口
        //ABPORT=2内网访问外网
        //ABPORT=3非异常数据且也是开放端口

        //判断内网访问外网
        if (SRCIP == ip) {
          PORT = SRCPORT
          ABPORT = "2"
          INDEX = "other"
        } else {
          PORT = DSTPORT
          ABPORT = "0"

        }
        //判断端口异常
        if (abportList.contains(DSTPORT) && ABPORT != "2" && PROTO == "TCP" && PACKERNUM.toInt >= 2) {
          ABPORT = "1"
          INDEX = "other"
        }
        //判断非异常且是否为开放端口
        if (ABPORT == "0" && PROTO == "TCP" || PROTO == "UDP" && PACKERNUM.toInt >= 2) {
          ABPORT = "3"
        }

        (recordid, clno, name, cltime, dataori, INDEX, COSTTIME, PACKERNUM, BYTESIZE, PROTO, RECORDTIME, SRCIP, DSTIP, PORT, ABPORT, DATE)
    }.toDF("ROW", "CLNO", "NAME", "CLTIME", "DATA_ORIGIN", "INDEX", "COSTTIME", "PACKERNUM", "BYTESIZE", "PROTO", "RECORDTIME", "SRCIP", "DSTIP", "PORT", "ABPORT", "DATE")
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

  //提取出流量行为基线的上下限
  def getBaseLine(ip: String, CLNO: String, TIME: String, dataFrame: DataFrame, spark: SparkSession): DataFrame = {

    logger.error("获取流量行为的行为基线")
    dataFrame.createOrReplaceTempView("BASELINE")

    //获取所有端口协议的流量连接上下限
    val calcAllSql = "select DATE,case when DATE != '' then '' end PORT,case when DATE != '' then '' end PROTO,case when (AVG(PACKERNUM)-3*STD(PACKERNUM))< 0 then 0 else (AVG(PACKERNUM)-3*STD(PACKERNUM)) end CONNECTDOWN,(AVG(PACKERNUM)+3*STD(PACKERNUM)) as CONNECTUP,case when (AVG(BYTESIZE)-3*STD(BYTESIZE))< 0 then 0 else (AVG(BYTESIZE)-3*STD(BYTESIZE)) end NETFLOWDOWN,(AVG(BYTESIZE)+3*STD(BYTESIZE)) as NETFLOWUP,case when DATE != '' then 'ALL' end BASELINE from BASELINE  where ABPORT=3 group by DATE"
    val allData = spark.sql("select * from (" + calcAllSql + ") T where CONNECTDOWN<>'NaN'")
    //获取端口的流量连接上下限
    val calcPortSql = "select DATE,PORT,case when DATE != '' then '' end PROTO,case when (AVG(PACKERNUM)-3*STD(PACKERNUM))< 0 then 0 else (AVG(PACKERNUM)-3*STD(PACKERNUM)) end CONNECTDOWN,(AVG(PACKERNUM)+3*STD(PACKERNUM)) as CONNECTUP,case when (AVG(BYTESIZE)-3*STD(BYTESIZE))< 0 then 0 else (AVG(BYTESIZE)-3*STD(BYTESIZE)) end NETFLOWDOWN,(AVG(BYTESIZE)+3*STD(BYTESIZE)) as NETFLOWUP,case when DATE != '' then 'PORT' end BASELINE from BASELINE where ABPORT=3 group by PORT,DATE"
    val portData = spark.sql("select * from (" + calcPortSql + ") T where CONNECTDOWN<>'NaN'")
    //获取协议的流量连接上下限
    val calcProtoSql = "select DATE,case when DATE != '' then '' end PORT,PROTO,case when (AVG(PACKERNUM)-3*STD(PACKERNUM)) < 0 then 0 else (AVG(PACKERNUM)-3*STD(PACKERNUM)) end CONNECTDOWN,(AVG(PACKERNUM)+3*STD(PACKERNUM)) as CONNECTUP,case when (AVG(BYTESIZE)-3*STD(BYTESIZE)) < 0 then 0 else (AVG(BYTESIZE)-3*STD(BYTESIZE)) end NETFLOWDOWN,(AVG(BYTESIZE)+3*STD(BYTESIZE)) as NETFLOWUP,case when DATE != '' then 'PROTO' end BASELINE from BASELINE  where ABPORT=3 group by PROTO,DATE"
    val protoData = spark.sql("select * from (" + calcProtoSql + ") T where CONNECTDOWN<>'NaN'")
    //获取某个端口协议的流量连接上下限
    val calcportProtoSql = "select DATE,PORT,PROTO,case when (AVG(PACKERNUM)-3*STD(PACKERNUM)) < 0 then 0 else (AVG(PACKERNUM)-3*STD(PACKERNUM)) end CONNECTDOWN,(AVG(PACKERNUM)+3*STD(PACKERNUM)) as CONNECTUP,case when (AVG(BYTESIZE)-3*STD(BYTESIZE)) < 0 then 0 else (AVG(BYTESIZE)-3*STD(BYTESIZE)) end NETFLOWDOWN,(AVG(BYTESIZE)+3*STD(BYTESIZE)) as NETFLOWUP,case when DATE != '' then 'PORTPROTO' end BASELINE from BASELINE  where ABPORT=3 group by PROTO,PORT,DATE"
    val portProtoData = spark.sql("select * from (" + calcportProtoSql + ") T where CONNECTDOWN<>'NaN'")
    //合并dataframe
    val dataFrameList: List[DataFrame] = List(allData, portData, protoData, portProtoData)
    val unionData = mergeDataFrame(dataFrameList)

    val resultData = addFields(ip, unionData, CLNO, TIME)
    resultData
  }

  //合并dataframe函数
  def mergeDataFrame(dataFrameList: List[DataFrame]): DataFrame = {
    val unionFun = (a: DataFrame, b: DataFrame) => a.union(b).toDF
    val unionData = dataFrameList.tail.foldRight(dataFrameList.head)(unionFun)
    unionData
  }

  //给合并后的dataframe增加
  def addFields(ip: String, dataFrame: DataFrame, CLNO: String, TIME: String): DataFrame = {
    import spark.implicits._
    val resultDF = dataFrame.rdd.map {
      row =>
        val recordid: String = ROWUtils.genaralROW()
        val clno: String = CLNO.replace("-", "")
        val cltime: String = TIME
        val name = ip
        val dataori: String = "s"

        val DATE = row.getAs[String]("DATE")
        val PORT = row.getAs[String]("PORT")
        val PROTO = row.getAs[String]("PROTO")
        val CONNECTAVG = "-2"
        val NETFLOWAVG = "-2"
        val CONNECTDOWN = row.getAs[Double]("CONNECTDOWN").toInt.toString
        val CONNECTUP = row.getAs[Double]("CONNECTUP").toInt.toString
        val NETFLOWDOWN = row.getAs[Double]("NETFLOWDOWN").toInt.toString
        val NETFLOWUP = row.getAs[Double]("NETFLOWUP").toInt.toString
        val BASELINE = row.getAs[String]("BASELINE")

        (recordid, clno, name, cltime, dataori, PROTO, PORT, DATE, CONNECTDOWN, CONNECTUP, NETFLOWDOWN, NETFLOWUP, CONNECTAVG, NETFLOWAVG, BASELINE)
    }.toDF("ROW", "CLNO", "NAME", "CLTIME", "DATA_ORIGIN", "PROTO", "PORT", "DATE", "CONNECTDOWN", "CONNECTUP", "NETFLOWDOWN", "NETFLOWUP", "CONNECTAVG", "NETFLOWAVG", "BASELINE")
    resultDF
  }
}

//日志系统防止序列化
trait LoggerSupport {
  @transient lazy val logger = Logger.getLogger(this.getClass)
}
