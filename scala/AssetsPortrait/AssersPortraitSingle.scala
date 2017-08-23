package AssetsPortraitSingle

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties, UUID}

import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import utils.ROWUtils
import utils.HDFSUtils

import scala.sys.process._
import java.io.{BufferedInputStream, File, FileInputStream}
import org.apache.spark.sql.functions.udf
import scala.util.Try
import com.github.nscala_time.time.Imports.{DateTime, DateTimeFormat}
import com.github.nscala_time.time.Imports._

/**
  * Created by bai on 2017/7/6.
  */


object AssersPortraitSingle {
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
  def readyMain() {
    val IPS = List("221.176.64.146", "221.176.64.166")
    val dataAll = getOriginalData(spark: SparkSession)
    val baseLineAll = getOriginalBaseLine(spark: SparkSession)

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
        val nwAllData = JudgeRule.getNwAbportData(ip, dataAll, CLNO, CLTIME, limitValue(dataAll.count()))
        val nwBaseLineData = JudgeRule.getNwAbportData(ip, baseLineAll, CLNO, CLTIME, limitValue(baseLineAll.count()))
        //获得上周的行为基线
        val baselineData = JudgeRule.judgeBaseLine(ip, CLNO, CLTIME, nwBaseLineData, spark)
        //临时基线
        //val baselineData = getHisBaseLine(CLNO,CLTIME,spark,ip)

        //获得昨天的端口异常等和行为基线
        val portData = JudgeRule.judgeAbnormal(nwAllData, baselineData, spark)
        val baseLineData = JudgeRule.getBaseLine(ip, CLNO, CLTIME, nwAllData, spark)

        logger.error("开始储存结果")
        //保存端口结果
        val AbPortSave = properties.getProperty("AbPortSave")
        val saveAbPortOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> AbPortSave)
        portData.write.format("com.databricks.spark.csv").mode(SaveMode.Append).options(saveAbPortOptions).save()
        //保存基线结果
        val BaseLineSave = properties.getProperty("BaseLineSave")
        val saveBaseLineOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> BaseLineSave)
        baseLineData.write.format("com.databricks.spark.csv").mode(SaveMode.Append).options(saveBaseLineOptions).save()
        //保存上周的基线结果
        baselineData.write.format("com.databricks.spark.csv").mode(SaveMode.Append).options(saveBaseLineOptions).save()
      }
    }

    logger.error("开始get到本地")
    val baseurl = properties.getProperty("hdfs.base.path")

    val srcpath_AbPort = baseurl + properties.getProperty("resultdata.path.AbPort")
    val dstpath_AbPort = properties.getProperty("file.dst.path.AbPort") + "T_SIEM_KMEANS_CLUSTER@" + getTime(-1) + "@ABPORT.csv"
    merge(srcpath_AbPort, dstpath_AbPort)

    val srcpath_BaseLine = baseurl + properties.getProperty("resultdata.path.BaseLine")
    val dstpath_BaseLine = properties.getProperty("file.dst.path.BaseLine") + "T_SIEM_KMEANS_CLUSTER@" + getTime(-1) + "@BASELINE.csv"
    merge(srcpath_BaseLine, dstpath_BaseLine)

    //生成标识文件
    val flagname_AbPort = "T_SIEM_KMEANS_CLUSTER@" + getTime(-1) + "@ABPORT.flag"
    val flagname_BaseLine = "T_SIEM_KMEANS_CLUSTER@" + getTime(-1) + "@BASELINE.flag"
    val fileidnet_AbPort = new File(properties.getProperty("file.dst.path.AbPort") + flagname_AbPort).createNewFile()
    val fileident_BaseLine = new File(properties.getProperty("file.dst.path.BaseLine") + flagname_BaseLine).createNewFile()

    //删除HDFS
    val deleteAbPort = properties.getProperty("deleteAbPort")
    logger.error("执行" + deleteAbPort)
    deleteAbPort.!!

    val deteleBaseLine = properties.getProperty("deteleBaseLine")
    logger.error("执行" + deteleBaseLine)
    deteleBaseLine.!!
  }

  //判断极限值
  def limitValue(count: Long): Int = {
    var limValue = (count.toInt * 0.00375).toInt
    if (limValue < 10) {
      limValue = 10
    }
    limValue
  }

  //获取ip的种类
  def getIPList(dataFrame: DataFrame, spark: SparkSession): Array[String] = {
    logger.error("获取ip种类开始")
    dataFrame.createOrReplaceTempView("NETFLOWDATA")
    val SRCIPS: Array[String] = spark.sql(s"SELECT DISTINCT DSTIP FROM NETFLOWDATA WHERE FLOWD='1' OR FLOWD='2'").rdd.map(_.toSeq.toList.head.toString).collect()
    SRCIPS
  }


  //读取原始数据并持久化
  def getOriginalData(spark: SparkSession): DataFrame = {
    logger.error("获取原始数据")
    val nowday = properties.getProperty("nowday").toInt
    val dataPath = properties.getProperty("Netflowpath") + getTime(nowday - 1) + "--" + getTime(nowday)
    val dataOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> dataPath)
    val dataAll = spark.read.options(dataOptions).format("com.databricks.spark.csv").load()
    dataAll.persist(StorageLevel.MEMORY_AND_DISK)
    dataAll
  }

  //生成计算时间
  def getNowTime(): String = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time = new Date().getTime
    format.format(time)
  }

  //获取日期
  def getTime(day: Int): String = {
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, day)
    val time: Date = cal.getTime
    val newtime: String = new SimpleDateFormat("yyyy-MM-dd").format(time)
    newtime
  }

  //读取基线原始数据
  def getOriginalBaseLine(spark: SparkSession): DataFrame = {
    logger.error("获取以前的基线范围数据")
    val nowday = properties.getProperty("nowday").toInt
    val dataPath = properties.getProperty("Netflowpath")
    val dataList: List[DataFrame] = (nowday - 8 to nowday - 2).map { each =>
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
    val toStringUdf = udf[String, Int](_.toString)
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
}

//判断规则的主类
class JudgeRuleClass(properties: Properties, spark: SparkSession) extends Serializable with LoggerSupport {
  //筛选出内外网、异常端口、开放端口、正常端口
  def getNwAbportData(ip: String, dataFrame: DataFrame, CLNO: String, TIME: String, counts: Int): DataFrame = {

    logger.error("筛选出内外网、异常端口、开放端口、正常端口开始")
    //筛选某个ip下的数据
    dataFrame.createOrReplaceTempView("NETFLOWDATA")
    val dataDF = spark.sql(s"SELECT COSTTIME,TCP,UDP,ICMP,GRE,PACKERNUM,BYTESIZE,PROTO,FLOWD,ROW,RECORDTIME,SRCIP,DSTIP,SRCPORT,DSTPORT FROM NETFLOWDATA WHERE DSTIP = '" + ip + "' OR SRCIP = '" + ip + "'")
      .toDF("COSTTIME", "TCP", "UDP", "ICMP", "GRE", "PACKERNUM", "BYTESIZE", "PROTO", "FLOWD", "ROW", "RECORDTIME", "SRCIP", "DSTIP", "SRCPORT", "DSTPORT")

    //筛选出有可能异常的端口
    dataDF.createOrReplaceTempView("PORTDATA")
    val abportData = spark.sql(s"SELECT DSTPORT FROM (SELECT DSTPORT,COUNT(DSTPORT) AS COUNTS FROM PORTDATA WHERE DSTIP = '" + ip + "' GROUP BY DSTPORT) AS T WHERE COUNTS < " + counts)
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
        val RECORDTIME = correctTime(row.getAs[String]("RECORDTIME"))
        val SRCIP = row.getAs[String]("SRCIP")
        val DSTIP = row.getAs[String]("DSTIP")
        val SRCPORT = row.getAs[String]("SRCPORT")
        val DSTPORT = row.getAs[String]("DSTPORT")
        val OLDRECORDTIME = row.getAs[String]("RECORDTIME")
        val DATE = fmtime(OLDRECORDTIME)

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
        } else if (DSTPORT.toInt < 1024 && PACKERNUM.toInt >= 2) {
          if (abportList.contains(DSTPORT)) {
            if (DSTPORT != "443" && DSTPORT != "80") {
              PORT = DSTPORT
              ABPORT = "1"
            } else {
              PORT = DSTPORT
              ABPORT = "0"
            }
          } else {
            PORT = DSTPORT
            ABPORT = "0"
          }
        } else if (PACKERNUM.toInt >= 2) {
          PORT = DSTPORT
          ABPORT = "1"
          INDEX = "other"
        }
        //判断端口异常
        if (abportList.contains(DSTPORT) && ABPORT != "2" && PROTO == "TCP" && PACKERNUM.toInt >= 3) {
          if (DSTPORT != "443" && DSTPORT != "80") {
            ABPORT = "1"
            INDEX = "other"
          }
        }
        //判断非异常且是否为开放端口
        if (ABPORT == "0" && PROTO == "TCP" || PROTO == "UDP" && PACKERNUM.toInt >= 2) {
          ABPORT = "3"
        }
        (recordid, clno, name, cltime, dataori, INDEX, COSTTIME, PACKERNUM, BYTESIZE, PROTO, RECORDTIME, SRCIP, DSTIP, PORT, ABPORT, DATE)
    }.toDF("ROW", "CLNO", "NAME", "CLTIME", "DATA_ORIGIN", "CENTER_INDEX", "TFTIME", "PACKERNUM", "BYTESIZE", "PROTO", "RECORDTIME", "SRCIP", "DSTIP", "PORT", "ABPORT", "DATE")
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

  //这里是修正recordtime时间的函数
  def correctTime(recordTime: String): String = {
    val oriDate = recordTime.substring(0, 19)
    val tempDate = DateTime.parse(oriDate, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")) + 8.hour
    val date = tempDate.toString().substring(0, 19).replace("T", " ")
    date
  }

  //提取出流量行为基线的均值
  def getBaseLine(ip: String, CLNO: String, TIME: String, dataFrame: DataFrame, spark: SparkSession): DataFrame = {

    logger.error("获取流量行为的行为基线")
    dataFrame.createOrReplaceTempView("BASELINE")
    //获取基线中的端口协议
    val portList: List[String] = spark.sql(s"select DISTINCT PORT from BASELINE where PORT is not null and PORT<>' ' and ABPORT=3").rdd.map(_.toSeq.toList.head.toString).collect().toList
    val protoList: List[String] = spark.sql(s"select DISTINCT PROTO from BASELINE where PROTO is not null and PROTO<>' ' and ABPORT=3").rdd.map(_.toSeq.toList.head.toString).collect().toList

    //获取所有端口协议的流量连接均值
    val calcAllSql = "select DATE,case when DATE != '' then '' end PORT,case when DATE != '' then '' end PROTO,case when (AVG(PACKERNUM))< 0 then 0.00 else (MAX(CAST(PACKERNUM AS double))) end CONNECTAVG,case when (AVG(BYTESIZE))< 0 then 0.00 else (MAX(CAST(BYTESIZE AS double))/1024) end NETFLOWAVG,case when DATE != '' then 'ALL' end BASELINE from BASELINE group by DATE"
    val allData = spark.sql("select * from (" + calcAllSql + ") T")
    //获取端口的流量连接均值
    val calcPortSql = "select DATE,PORT,case when DATE != '' then '' end PROTO,case when (AVG(PACKERNUM))< 0 then 0.00 else (MAX(CAST(PACKERNUM AS double))) end CONNECTAVG,case when (AVG(BYTESIZE))< 0 then 0.00 else (MAX(CAST(BYTESIZE AS double))/1024) end NETFLOWAVG,case when DATE != '' then 'PORT' end BASELINE from BASELINE group by PORT,DATE"
    val portData = spark.sql("select * from (" + calcPortSql + ") T")
    //获取协议的流量连接均值
    val calcProtoSql = "select DATE,case when DATE != '' then '' end PORT,PROTO,case when (AVG(PACKERNUM)) < 0 then 0.00 else (MAX(CAST(PACKERNUM AS double))) end CONNECTAVG,case when (AVG(BYTESIZE)) < 0 then 0.00 else (MAX(CAST(BYTESIZE AS double))/1024) end NETFLOWAVG,case when DATE != '' then 'PROTO' end BASELINE from BASELINE group by PROTO,DATE"
    val protoData = spark.sql("select * from (" + calcProtoSql + ") T")
    //获取某个端口协议的流量连接均值
    val calcportProtoSql = "select DATE,PORT,PROTO,case when (AVG(PACKERNUM)) < 0 then 0.00 else (MAX(CAST(PACKERNUM AS double))) end CONNECTAVG,case when (AVG(BYTESIZE)) < 0 then 0.00 else (MAX(CAST(BYTESIZE AS double))/1024) end NETFLOWAVG,case when DATE != '' then 'PORTPROTO' end BASELINE from BASELINE group by PROTO,PORT,DATE"
    val portProtoData = spark.sql("select * from (" + calcportProtoSql + ") T")
    //合并dataframe
    val dataFrameList: List[DataFrame] = List(allData, portData, protoData, portProtoData)
    val unionData = mergeDataFrame(dataFrameList)
    val resultData = addFields(ip, unionData, CLNO, TIME, portList, protoList)
    resultData
  }

  //合并dataframe函数
  def mergeDataFrame(dataFrameList: List[DataFrame]): DataFrame = {
    val unionFun = (a: DataFrame, b: DataFrame) => a.union(b).toDF
    val unionData = dataFrameList.tail.foldRight(dataFrameList.head)(unionFun)
    unionData
  }

  //给合并后的dataframe增加字段
  def addFields(ip: String, dataFrame: DataFrame, CLNO: String, TIME: String, portList: List[String], protoList: List[String]): DataFrame = {
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
        val CONNECTAVG = row.getAs[Double]("CONNECTAVG").formatted("%.2f")
        val NETFLOWAVG = row.getAs[Double]("NETFLOWAVG").formatted("%.2f")
        val CONNECTDOWN = "-3.0"
        val CONNECTUP = "-3.0"
        val NETFLOWDOWN = "-3.0"
        val NETFLOWUP = "-3.0"
        val BASELINE = row.getAs[String]("BASELINE")

        var judge = 0
        if (PORT == "" && PROTO == "" && BASELINE == "ALL") {
          judge = 1
        } else if (portList.contains(PORT) && PROTO == "" && BASELINE == "PORT") {
          judge = 1
        } else if (PORT == "" && protoList.contains(PROTO) && BASELINE == "PROTO") {
          judge = 1
        } else if (portList.contains(PORT) && protoList.contains(PROTO) && BASELINE == "PORTPROTO") {
          judge = 1
        } else {
          judge = 0
        }

        (recordid, clno, name, cltime, dataori, PROTO, PORT, DATE, CONNECTDOWN, CONNECTUP, NETFLOWDOWN, NETFLOWUP, CONNECTAVG, NETFLOWAVG, BASELINE, judge)
    }.toDF("ROW", "CLNO", "NAME", "CLTIME", "DATA_ORIGIN", "PROTO", "PORT", "DATE", "CONNECTDOWN", "CONNECTUP", "NETFLOWDOWN", "NETFLOWUP", "CONNECTAVG", "NETFLOWAVG", "BASELINE", "judge")

    //筛选出需要的结果
    resultDF.createOrReplaceTempView("RESULT")
    spark.sql(s"select ROW, CLNO, NAME, CLTIME, DATA_ORIGIN, PROTO, PORT, DATE, CONNECTDOWN, CONNECTUP, NETFLOWDOWN, NETFLOWUP, CONNECTAVG, NETFLOWAVG, BASELINE from RESULT where judge=1")
  }

  //判断基线不正常的数据
  def judgeAbnormal(nwAllData: DataFrame, baselineData: DataFrame, spark: SparkSession): DataFrame = {

    //得到持久化的基线范围
    baselineData.createOrReplaceTempView("LONGBASELINE")
    //获取基线中的端口协议
    val portList = spark.sql(s"select DISTINCT PORT from LONGBASELINE where PORT is not null and PORT<>''").rdd.map(_.toSeq.toList.head.toString).collect().toList
    val protoList = spark.sql(s"select DISTINCT PROTO from LONGBASELINE where PROTO is not null and PROTO<>''").rdd.map(_.toSeq.toList.head.toString).collect().toList

    val maxConnect = spark.sql(s"select CONNECTUP from LONGBASELINE where CONNECTUP is not null and CONNECTUP<>'' and BASELINE='ALL'").rdd.map(_.toSeq.toList.head.toString).collect().head
    val maxNetflow = spark.sql(s"select NETFLOWUP from LONGBASELINE where NETFLOWUP is not null and NETFLOWUP<>'' and BASELINE='ALL'").rdd.map(_.toSeq.toList.head.toString).collect().head

    logger.error("获取基线中的端口、协议、流量范围")

    nwAllData.createOrReplaceTempView("PORTDATA")
    val joinSql = "PORTDATA.NAME=LONGBASELINE.NAME and PORTDATA.PORT=LONGBASELINE.PORT and PORTDATA.PROTO=LONGBASELINE.PROTO"
    val joinDF = spark.sql(s"select PORTDATA.*,CONNECTDOWN,CONNECTUP,NETFLOWDOWN,NETFLOWUP from PORTDATA left join LONGBASELINE on " + joinSql)

    import spark.implicits._
    val ResultDF = joinDF.rdd.map {
      row =>
        val ROW = row.getAs[String]("ROW")
        val CLNO = row.getAs[String]("CLNO")
        val NAME = row.getAs[String]("NAME")
        val CLTIME = row.getAs[String]("CLTIME")
        val DATA_ORIGIN = row.getAs[String]("DATA_ORIGIN")
        var CENTER_INDEX = row.getAs[String]("CENTER_INDEX")
        val TFTIME = row.getAs[String]("TFTIME")
        val PACKERNUM = row.getAs[String]("PACKERNUM")
        val BYTESIZE = row.getAs[String]("BYTESIZE")
        val PROTO = row.getAs[String]("PROTO")
        val RECORDTIME = row.getAs[String]("RECORDTIME")
        val SRCIP = row.getAs[String]("SRCIP")
        val DSTIP = row.getAs[String]("DSTIP")
        val PORT = row.getAs[String]("PORT")
        val ABPORT = row.getAs[String]("ABPORT")
        val DATE = row.getAs[String]("DATE")
        val CONNECTDOWN = row.getAs[String]("CONNECTDOWN")
        val CONNECTUP = row.getAs[String]("CONNECTUP")
        val NETFLOWDOWN = row.getAs[String]("NETFLOWDOWN")
        val NETFLOWUP = row.getAs[String]("NETFLOWUP")

        //CENTER_INDEX=other为内外网端口异常
        //CENTER_INDEX=portabnormal基线端口异常
        //CENTER_INDEX=protoabnormal基线协议异常
        //CENTER_INDEX=connectabnormal基线连接次数异常
        //CENTER_INDEX=netflowabnormal基线流量大小异常
        //CENTER_INDEX=normall基线数据正常

        //判断使用不常用端口
        if (ABPORT == "3") {
          if (portList.contains(PORT)) {} else {
            CENTER_INDEX = "portabnormal"
          }
        }
        //判断使用不常用协议
        if (ABPORT == "3") {
          if (protoList.contains(PROTO)) {} else {
            CENTER_INDEX = "protoabnormal"
          }
        }
        //判断是否在流量范围内
        if (ABPORT == "3" && CENTER_INDEX == "") {
          if (Try(BYTESIZE.toDouble / 1024).getOrElse(0.0) > Try(maxNetflow.toDouble).getOrElse(0.0)) {
            CENTER_INDEX = "netflowabnormal"
          }
        }
        //判断是否在连接范围内
        if (ABPORT == "3" && CENTER_INDEX == "") {
          val pack = Try(PACKERNUM.toDouble).getOrElse(0.0)
          if (pack > Try(maxConnect.toDouble).getOrElse(0.0)) {
            CENTER_INDEX = "connectabnormal"
          } else if (pack == 0.0 || maxConnect == "0.0") {
            CENTER_INDEX = "normal"
          }
        }
        //打上normal标签
        if (ABPORT == "3" && CENTER_INDEX == "") {
          CENTER_INDEX = "normal"
        }

        (ROW, CLNO, NAME, CLTIME, DATA_ORIGIN, CENTER_INDEX, TFTIME, PACKERNUM, BYTESIZE, PROTO, RECORDTIME, SRCIP, DSTIP, PORT, ABPORT, DATE)
    }.toDF("ROW", "CLNO", "NAME", "CLTIME", "DATA_ORIGIN", "CENTER_INDEX", "TFTIME", "PACKERNUM", "BYTESIZE", "PROTO", "RECORDTIME", "SRCIP", "DSTIP", "PORT", "ABPORT", "DATE")
    ResultDF
  }

  //得到以前的行为基线
  def judgeBaseLine(ip: String, CLNO: String, TIME: String, nwBaseLineData: DataFrame, spark: SparkSession): DataFrame = {
    nwBaseLineData.createOrReplaceTempView("NWBASELINEDATA")
    //获取基线中的端口协议
    val portList: List[String] = spark.sql(s"select DISTINCT PORT from NWBASELINEDATA where PORT is not null and PORT<>' ' and ABPORT=3").rdd.map(_.toSeq.toList.head.toString).collect().toList
    val protoList: List[String] = spark.sql(s"select DISTINCT PROTO from NWBASELINEDATA where PROTO is not null and PROTO<>' ' and ABPORT=3").rdd.map(_.toSeq.toList.head.toString).collect().toList

    //获取所有端口协议的流量连接上下限3*STD(PACKERNUM)
    val calcAllSql = "select NAME,case when NAME != '' then '' end PORT,case when NAME != '' then '' end PROTO,case when AVG(PACKERNUM) > 0 then 0.00 else 0.00 end CONNECTDOWN,(3*AVG(CAST(PACKERNUM AS int))) as CONNECTUP,case when AVG(BYTESIZE)> 0 then 0 else 0 end NETFLOWDOWN,(3*AVG(CAST(BYTESIZE AS double))/1024) as NETFLOWUP,case when NAME != '' then 'ALL' end BASELINE from NWBASELINEDATA group by NAME"
    val allData = spark.sql("select * from (" + calcAllSql + ") T")
    //获取端口的流量连接上下限
    val calcPortSql = "select NAME,PORT,case when NAME != '' then '' end PROTO,case when AVG(PACKERNUM)> 0 then 0.00 else 0.00 end CONNECTDOWN,(3*AVG(CAST(PACKERNUM AS int))) as CONNECTUP,case when AVG(BYTESIZE)> 0 then 0.00 else 0.00 end NETFLOWDOWN,(3*AVG(CAST(BYTESIZE AS double))/1024) as NETFLOWUP,case when NAME != '' then 'PORT' end BASELINE from NWBASELINEDATA group by PORT,NAME"
    val portData = spark.sql("select * from (" + calcPortSql + ") T")
    //获取协议的流量连接上下限
    val calcProtoSql = "select NAME,case when NAME != '' then '' end PORT,PROTO,case when AVG(PACKERNUM)> 0 then 0.00 else 0.00 end CONNECTDOWN,(3*AVG(CAST(PACKERNUM AS int))) as CONNECTUP,case when AVG(BYTESIZE)> 0 then 0.00 else 0.00 end NETFLOWDOWN,(3*AVG(CAST(BYTESIZE AS double))/1024) as NETFLOWUP,case when NAME != '' then 'PROTO' end BASELINE from NWBASELINEDATA group by PROTO,NAME"
    val protoData = spark.sql("select * from (" + calcProtoSql + ") T")
    //获取某个端口协议的流量连接上下限
    val calcportProtoSql = "select NAME,PORT,PROTO,case when AVG(PACKERNUM)> 0 then 0.00 else 0.00 end CONNECTDOWN,(3*AVG(CAST(PACKERNUM AS int))) as CONNECTUP,case when AVG(BYTESIZE)> 0 then 0 else 0 end NETFLOWDOWN,(3*AVG(CAST(BYTESIZE AS double))/1024) as NETFLOWUP,case when NAME != '' then 'PORTPROTO' end BASELINE from NWBASELINEDATA group by PROTO,PORT,NAME"
    val portProtoData = spark.sql("select * from (" + calcportProtoSql + ") T ")
    //合并dataframe
    val dataFrameList: List[DataFrame] = List(allData, portData, protoData, portProtoData)
    val unionData = mergeDataFrame(dataFrameList)

    val resultData = addBaseLineFields(ip, unionData, CLNO, TIME, portList, protoList)
    resultData
  }

  //给合并后的dataframe增加
  def addBaseLineFields(ip: String, dataFrame: DataFrame, CLNO: String, TIME: String, portList: List[String], protoList: List[String]): DataFrame = {
    import spark.implicits._
    val resultDF = dataFrame.rdd.map {
      row =>
        val recordid: String = ROWUtils.genaralROW()
        val clno: String = CLNO.replace("-", "")
        val cltime: String = TIME
        val name = ip
        val dataori: String = "s"

        val DATE = ""
        val PORT = row.getAs[String]("PORT")
        val PROTO = row.getAs[String]("PROTO")
        val CONNECTAVG = "-2.0"
        val NETFLOWAVG = "-2.0"
        val CONNECTDOWN = row.getAs[Double]("CONNECTDOWN").formatted("%.2f")
        val CONNECTUP = row.getAs[Double]("CONNECTUP").formatted("%.2f")
        val NETFLOWDOWN = row.getAs[Double]("NETFLOWDOWN").formatted("%.2f")
        val NETFLOWUP = row.getAs[Double]("NETFLOWUP").formatted("%.2f")
        val BASELINE = row.getAs[String]("BASELINE")

        var judge = 0
        if (PORT == "" && PROTO == "" && BASELINE == "ALL") {
          judge = 1
        } else if (portList.contains(PORT) && PROTO == "" && BASELINE == "PORT") {
          judge = 1
        } else if (PORT == "" && protoList.contains(PROTO) && BASELINE == "PROTO") {
          judge = 1
        } else if (portList.contains(PORT) && protoList.contains(PROTO) && BASELINE == "PORTPROTO") {
          judge = 1
        } else {
          judge = 0
        }


        (recordid, clno, name, cltime, dataori, PROTO, PORT, DATE, CONNECTDOWN, CONNECTUP, NETFLOWDOWN, NETFLOWUP, CONNECTAVG, NETFLOWAVG, BASELINE, judge)
    }.toDF("ROW", "CLNO", "NAME", "CLTIME", "DATA_ORIGIN", "PROTO", "PORT", "DATE", "CONNECTDOWN", "CONNECTUP", "NETFLOWDOWN", "NETFLOWUP", "CONNECTAVG", "NETFLOWAVG", "BASELINE", "judge")
    //筛选出需要的结果
    resultDF.createOrReplaceTempView("RESULT")
    spark.sql(s"select ROW, CLNO, NAME, CLTIME, DATA_ORIGIN, PROTO, PORT, DATE, CONNECTDOWN, CONNECTUP, NETFLOWDOWN, NETFLOWUP, CONNECTAVG, NETFLOWAVG, BASELINE from RESULT where judge=1")
  }

}

//日志系统防止序列化
trait LoggerSupport {
  @transient lazy val logger = Logger.getLogger(this.getClass)
}