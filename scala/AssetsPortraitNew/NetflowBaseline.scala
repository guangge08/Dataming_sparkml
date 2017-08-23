import java.io.{BufferedInputStream, FileInputStream}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties, UUID}

import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.elasticsearch.spark.sql._
import utils.ROWUtils

import scala.util.Try

/**
  * Created by bai on 2017/7/19.
  */
object NetflowBaseline {
  def main(args: Array[String]): Unit = {

    //配置日志文件
    val filePath = System.getProperty("user.dir")
    PropertyConfigurator.configure(filePath + "/conf/log4jtyb.properties")

    //读取配置文件
    val properties: Properties = new Properties()
    val ipstream = new BufferedInputStream(new FileInputStream(filePath + "/conf/netflowbaseline.properties"))
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
      .set("es.nodes", properties.getProperty("es.nodes"))
      .set("es.port", properties.getProperty("es.port"))
    val Spark = SparkSession.builder().config(sparkconf).getOrCreate()
    Spark
  }
}


/**
  * 主函数的准备函数
  */
class ReadyMainClass(properties: Properties, spark: SparkSession) extends LoggerSupport {
  //准备函数的主函数
  def readyMain(): Unit = {

    //从ES里面读取数据并储存到HDFS中
    val NetflowGetData = new NetflowGetDataClass(properties, spark)
    //NetflowGetData.getDataMain()

    //生成计算时间
    val CLTIME = new Date().getTime

    //获取一天的Netflow数据
    val dayData = getDayDataByHDFS()
    //获取一周的Netflow数据
    val weekData = getWeekDataByHDFS()

    //声明处理计算数据的类
    val FormatData = new FormatDataClass(properties, spark)

    //保存结果的路径
    val saveEsPath = properties.getProperty("saveEsPath")

    //获取一周的IP种类
    var IPS: List[String] = Nil
    //配置读取IP项
    val ips = properties.getProperty("ips")
    if (ips == "true") {
      IPS = getIPList(weekData).toList
    } else {
      IPS = ips.split(",").toList
    }

    IPS.foreach {
      ip =>
        logger.error("计算ip" + ip + "开始")
        //计算基线部分
        val CLNO = UUID.randomUUID().toString
        //计算一天的行为基线
        val dayBaseline = FormatData.getDayBaseLine(dayData, ip, CLNO, CLTIME)

        //计算一周的行为基线
        val weekBaseline = FormatData.getWeekBaseLine(weekData, ip, CLNO, CLTIME)

        //储存到ES中
        dayBaseline.saveToEs(saveEsPath)
        weekBaseline.saveToEs(saveEsPath)

        //计算元数据部分
        val srcData = FormatData.judgeAbnormal(dayData, weekBaseline, CLNO, CLTIME)

        //储存到ES中
        srcData.saveToEs(saveEsPath)
        logger.error("保存到ES")
    }
  }

  //获取昨天的NetFlow数据
  def getDayDataByHDFS(): DataFrame = {
    //读取HDFS获得所有数据并持久化
    val datapath = properties.getProperty("dayDataPath")
    val dataoptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> datapath)
    val dayData = spark.read.options(dataoptions).format("com.databricks.spark.csv").load()
    dayData.persist(StorageLevel.MEMORY_AND_DISK)
    dayData
  }

  //获取一周的NetFlow数据
  def getWeekDataByHDFS(): DataFrame = {
    //读取HDFS获得所有数据并持久化
    val datapath = properties.getProperty("weekDataPath")
    val dataoptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> datapath)
    val weekData = spark.read.options(dataoptions).format("com.databricks.spark.csv").load()
    weekData.persist(StorageLevel.MEMORY_AND_DISK)
    weekData
  }

  //获取ip的种类
  def getIPList(weekData: DataFrame): Array[String] = {
    logger.error("获取ip种类开始")
    weekData.createOrReplaceTempView("NETFLOWDATA")
    val SRCIPS: Array[String] = spark.sql(s"SELECT DISTINCT srcip FROM NETFLOWDATA").rdd.map(_.toSeq.toList.head.toString).collect()
    SRCIPS
  }
}

/**
  * 格式化原始数据
  * 一个处理计算数据的类
  */
class FormatDataClass(properties: Properties, spark: SparkSession) extends LoggerSupport with Serializable {

  //提取一天的行为基线
  def getDayBaseLine(dayData: DataFrame, ip: String, CLNO: String, TIME: Long): DataFrame = {

    logger.error("获取一天流量行为的行为基线")
    dayData.createOrReplaceTempView("DAYBASELINE")

    //获取所有协议下的流量、连接次数最大值
    val calcAllSql = "select srcip as name,case when date != '' then '' end proto,date,(max(cast(packernum as double))) as connectavg,(max(cast(bytesize as double))/1024) as netflowavg,case when date != '' then 'all' end baseline from DAYBASELINE group by srcip,date"
    val allData = spark.sql(calcAllSql)

    //获取某个协议下的流量、连接次数最大值
    val calcProtoSql = "select srcip as name,proto,date,(max(cast(packernum as double))) as connectavg,(max(cast(bytesize as double))/1024) as netflowavg,case when date != '' then 'proto' end baseline from DAYBASELINE group by srcip,proto,date"
    val protoData = spark.sql(calcProtoSql)

    //合并dataframe
    val dataFrameList: List[DataFrame] = List(allData, protoData)
    val unionData = mergeDataFrame(dataFrameList)
    val resultData = addFieldsDay(unionData, CLNO, TIME)
    resultData
  }

  //提取一周的行为基线
  def getWeekBaseLine(WeekData: DataFrame, ip: String, CLNO: String, TIME: Long): DataFrame = {

    logger.error("获取一周流量行为的行为基线")
    WeekData.createOrReplaceTempView("DAYBASELINE")

    //计算出3倍均值
    val allPackernum = spark.sql(s"select 3*avg(cast(packernum as double)) from DAYBASELINE").rdd.map(_.toSeq.toList.head.toString.toDouble).collect().head
    val allBytesize = spark.sql(s"select 3*avg(cast(bytesize as double)/1024) from DAYBASELINE").rdd.map(_.toSeq.toList.head.toString.toDouble).collect().head

    //获取所有协议下的流量、连接次数最大值
    val calcAllSql = s"select srcip as name,case when date != '' then '' end proto,date,case when std(cast(packernum as double))='NaN' then $allPackernum else (avg(cast(packernum as double)) + 3*std(cast(packernum as double))) end connectup,case when std(cast(bytesize as double))='NaN' then $allBytesize else (avg(cast(bytesize as double)/1024) + 3*std(cast(bytesize as double))/1024) end netflowup,case when date != '' then 'all' end baseline from DAYBASELINE group by srcip,date"
    val allData = spark.sql(calcAllSql)
    //获取某个协议下的流量、连接次数最大值
    val calcProtoSql = s"select srcip as name,proto,date,case when std(cast(packernum as double))='NaN' then $allPackernum else (avg(cast(packernum as double)) + 3*std(cast(packernum as double))) end connectup,case when std(cast(bytesize as double))='NaN' then $allBytesize else (avg(cast(bytesize as double)/1024) + 3*std(bytesize)/1024) end netflowup,case when date != '' then 'proto' end baseline from DAYBASELINE group by srcip,proto,date"
    val protoData = spark.sql(calcProtoSql)

    //合并dataframe
    val dataFrameList: List[DataFrame] = List(allData, protoData)
    val unionData = mergeDataFrame(dataFrameList)
    val resultData = addFieldsWeek(unionData, CLNO, TIME)
    resultData
  }

  //合并dataframe的函数
  def mergeDataFrame(dataFrameList: List[DataFrame]): DataFrame = {
    val unionFun = (a: DataFrame, b: DataFrame) => a.union(b).toDF
    val unionData = dataFrameList.tail.foldRight(dataFrameList.head)(unionFun)
    unionData
  }

  //给合并后的dataframe增加字段
  def addFieldsDay(dataFrame: DataFrame, CLNO: String, TIME: Long): DataFrame = {
    import spark.implicits._
    val resultDF = dataFrame.rdd.map {
      row =>
        val recordid: String = ROWUtils.genaralROW()
        val clno: String = CLNO.replace("-", "")
        val name = row.getAs[String]("name")
        val cltime: Long = TIME
        val index: String = "daybaseline"
        val proto = row.getAs[String]("proto")
        val date = row.getAs[String]("date")
        val connectdown = -3.0.toFloat
        val connectup = -3.0.toFloat
        val netflowdown = -3.0.toFloat
        val netflowup = -3.0.toFloat
        val connectavg = row.getAs[Double]("connectavg").toFloat
        val netflowavg = row.getAs[Double]("netflowavg").toFloat
        val baseline = row.getAs[String]("baseline")

        (recordid, clno, name, cltime, index, proto, date, connectdown, connectup, netflowdown, netflowup, connectavg, netflowavg, baseline)
    }.toDF("row", "clno", "name", "cltime", "index", "proto", "date", "connectdown", "connectup", "netflowdown", "netflowup", "connectavg", "netflowavg", "baseline")
    resultDF
  }

  //给合并后的dataframe增加字段
  def addFieldsWeek(dataFrame: DataFrame, CLNO: String, TIME: Long): DataFrame = {
    import spark.implicits._
    val resultDF = dataFrame.rdd.map {
      row =>
        val recordid: String = ROWUtils.genaralROW()
        val clno: String = CLNO.replace("-", "")
        val name = row.getAs[String]("name")
        val cltime: Long = TIME
        val index: String = "weekbaseline"
        val proto = row.getAs[String]("proto")
        val date = row.getAs[String]("date")
        val connectdown = -2.0.toFloat
        val connectup = row.getAs[Double]("connectup").toFloat
        val netflowdown = -2.0.toFloat
        val netflowup = row.getAs[Double]("netflowup").toFloat
        val connectavg = -2.0.toFloat
        val netflowavg = -2.0.toFloat
        val baseline = row.getAs[String]("baseline")

        (recordid, clno, name, cltime, index, proto, date, connectdown, connectup, netflowdown, netflowup, connectavg, netflowavg, baseline)
    }.toDF("row", "clno", "name", "cltime", "index", "proto", "date", "connectdown", "connectup", "netflowdown", "netflowup", "connectavg", "netflowavg", "baseline")
    resultDF
  }

  //判断基线不正常的数据
  def judgeAbnormal(dayData: DataFrame, weekBaseline: DataFrame, CLNO: String, TIME: Long): DataFrame = {
    //得到持久化的基线范围
    weekBaseline.createOrReplaceTempView("WEEKBASELINE")
    logger.error("获取基线中的协议、流量范围")
    //获取基线中的协议
    val protoList = spark.sql(s"select DISTINCT proto from WEEKBASELINE where proto is not null and proto<>' '").rdd.map(_.toSeq.toList.head.toString).collect().toList

    val maxConnect = spark.sql(s"select CONNECTUP from LONGBASELINE where CONNECTUP is not null and CONNECTUP<>'' and BASELINE='ALL'").rdd.map(_.toSeq.toList.head.toString).collect().head
    val maxNetflow = spark.sql(s"select NETFLOWUP from LONGBASELINE where NETFLOWUP is not null and NETFLOWUP<>'' and BASELINE='ALL'").rdd.map(_.toSeq.toList.head.toString).collect().head

    dayData.createOrReplaceTempView("DAYDATA")
    val joinSql = "DAYDATA.srcip=WEEKBASELINE.name and DAYDATA.proto=WEEKBASELINE.proto and DAYDATA.date=WEEKBASELINE.date"
    val joinDF = spark.sql(s"select DAYDATA.*,connectup,netflowup from DAYDATA left join WEEKBASELINE on " + joinSql)

    import spark.implicits._
    val resultDF = joinDF.rdd.map {
      row =>
        val recordid: String = ROWUtils.genaralROW()
        val clno: String = CLNO.replace("-", "")
        val name = row.getAs[String]("srcip")
        val cltime: Long = TIME
        var index: String = ""
        val packernum: Int = row.getAs[String]("packernum").toInt
        val bytesize: Int = row.getAs[String]("bytesize").toInt
        val proto = row.getAs[String]("proto")
        val date = row.getAs[String]("date")
        val recordtime = row.getAs[String]("recordtime")
        val connetup = row.getAs[Float]("connectup")
        val netflowup = row.getAs[Float]("netflowup")

        //判断协议异常
        if (protoList.contains(proto)) {} else {
          index = "protoabnormal"
        }
        //判断流量异常
        //判断是否在流量范围内
        if (index == "") {
          if (Try(bytesize.toDouble / 1024).getOrElse(0.0) > Try(maxNetflow.toDouble).getOrElse(0.0)) {
            index = "netflowabnormal"
          }
        }
        //判断连接次数异常
        if (index=="") {
          val pack = Try(packernum.toDouble).getOrElse(0.0)
          if (pack > Try(maxConnect.toDouble).getOrElse(0.0)) {
            index = "connectabnormal"
          } else if (pack == 0.0 || maxConnect == "0.0") {
            index = "normal"
          }
        }

        //打上normal标签
        if (index == "") {
          index = "normal"
        }
        (recordid, clno, name, cltime, index, packernum, bytesize, proto, date, recordtime)
    }.toDF("row", "clno", "name", "cltime", "index", "packernum", "bytesize", "proto", "date", "recordtime")
    resultDF
  }

}

/**
  * 从ES读取原始数据到HDFS
  */
class NetflowGetDataClass(properties: Properties, spark: SparkSession) extends LoggerSupport with Serializable {

  //读取es的数据并储存到HDFS
  def getDataMain(): Unit = {
    //从es中获取到一天的数据
    val dayData = getDayDataByES()
    logger.error("一天数据开始写入HDFS")
    if (dayData != null && dayData.take(10).length != 0) {
      val dataPath = properties.getProperty("dayDataPath")
      val saveOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> dataPath)
      dayData.write.options(saveOptions).mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save()
      logger.error("写入一天的数据到HDFS成功")
    }

    //从es中获取到一周的数据
    val weekData = getWeekDataByES()
    logger.error("一周数据开始写入HDFS")
    if (weekData != null && weekData.take(10).length != 0) {
      val dataPath = properties.getProperty("weekDataPath")
      val saveOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> dataPath)
      weekData.write.options(saveOptions).mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save()
      logger.error("写入一周的数据到HDFS成功")
    }

  }

  //获取昨天的NetFlow数据
  def getDayDataByES(): DataFrame = {

    val netflowIndex = properties.getProperty("netflowIndex")
    val nowDay = properties.getProperty("nowDay").toInt
    //抓取时间范围
    val start = Timestamp.valueOf(getTime(nowDay - 1)).getTime
    val end = Timestamp.valueOf(getTime(nowDay - 0)).getTime
    val query = "{\"query\":{\"range\":{\"recordtime\":{\"gte\":" + start + ",\"lte\":" + end + "}}}}"
    //抓取数据下来
    val rowDF = spark.esDF(netflowIndex, query)

    //转换时间格式
    val timeTransUDF = org.apache.spark.sql.functions.udf(
      (time: Long) => {
        val timestring = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time)
        fmTime(timestring)
      }
    )
    val netDF = rowDF.withColumn("timeTrans", timeTransUDF(rowDF("recordtime")))
    netDF.createOrReplaceTempView("netflow")
    //提取需要的数据出来
    val dataNET = spark.sql(
      """
        |SELECT srcip,dstip,cast(srcport as int),cast(dstport as int),proto,
        |cast(packernum as int),cast(bytesize as int),timeTrans as date,recordtime
        |FROM netflow
      """.stripMargin)
    dataNET
  }

  //获取一周的NetFlow数据
  def getWeekDataByES(): DataFrame = {

    val netflowIndex = properties.getProperty("netflowIndex")
    val nowDay = properties.getProperty("nowDay").toInt
    //抓取时间范围
    val start = Timestamp.valueOf(getTime(nowDay - 8)).getTime
    val end = Timestamp.valueOf(getTime(nowDay - 2)).getTime
    val query = "{\"query\":{\"range\":{\"recordtime\":{\"gte\":" + start + ",\"lte\":" + end + "}}}}"
    //抓取数据下来
    val rowDF = spark.esDF("netflow/netflow", query)
    //转换时间格式
    val timeTransUDF = org.apache.spark.sql.functions.udf(
      (time: Long) => {
        val timestring = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time)
        fmTime(timestring)
      }
    )
    val netDF = rowDF.withColumn("timeTrans", timeTransUDF(rowDF("recordtime")))
    netDF.createOrReplaceTempView("netflow")
    //提取需要的数据出来
    val dataNET = spark.sql(
      """
        |SELECT srcip,dstip,cast(srcport as int),cast(dstport as int),proto,
        |cast(packernum as int),cast(bytesize as int),timeTrans as date,recordtime
        |FROM netflow
      """.stripMargin)
    dataNET
  }

  //获取时间
  def getTime(day: Int): String = {
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, day)
    val time: Date = cal.getTime
    val newtime: String = new SimpleDateFormat("yyyy-MM-dd" + " 00:00:00").format(time)
    newtime
  }

  //这里是处理数据库时间的函数
  def fmTime(timestring: String): String = {
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
  * 日志系统防止序列化
  */
trait LoggerSupport {
  @transient lazy val logger = Logger.getLogger(this.getClass)
}