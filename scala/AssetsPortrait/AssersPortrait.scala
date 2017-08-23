package AssetsPortrait

import java.io.{BufferedInputStream, FileInputStream}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties, UUID}

import utils.ROWUtils
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, SparkSession, _}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}

/**
  * Created by bai on 2017/4/6.
  */


object AssersPortrait {
  //相当于全局变量
  //  val logger = Logger.getLogger("org")
  val logger = Logger.getLogger(this.getClass)
  //logger.setLevel(Level.INFO)

  val spark = getSparkSession()

  //读取配置文件
  //val properties: Properties = new Properties()
  //val ipstream: InputStream = this.getClass().getResourceAsStream("/config.properties")
  //properties.load(ipstream)

  val filePath = System.getProperty("user.dir")
  val properties: Properties = new Properties()
  val ipstream = new BufferedInputStream(new FileInputStream(filePath + "/conf/configkmean.properties"))
  properties.load(ipstream)

  //读取数据形成dataframe
  //val MoniflowpathIP = "hdfs://172.16.12.38:9000/spark/moniflowHDFS/2017-04-13"

  val NetflowpathIP = properties.getProperty("NetflowpathIP") + getStartTime(-1)  //加上昨天的日期
  val MoniflowpathIP = properties.getProperty("MoniflowpathIP")+ getStartTime(-1)
  //读取netflow
  val netflowoptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> NetflowpathIP)
  val netflowdataall = spark.read.options(netflowoptions).format("com.databricks.spark.csv").load()
  netflowdataall.persist(StorageLevel.MEMORY_AND_DISK)
  //读取moniflow
  val moniflowoptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> MoniflowpathIP)
  val moniflowdataall = spark.read.options(moniflowoptions).format("com.databricks.spark.csv").load()
  moniflowdataall.persist(StorageLevel.MEMORY_AND_DISK)

  //获取昨天的日期
  def getStartTime(day: Int): String = {
    val now = getNowTime()
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, day)
    val time: Date = cal.getTime
    val newtime: String = new SimpleDateFormat("yyyy-MM-dd").format(time)
    newtime
  }

  //建立sc
  def getSparkSession(): SparkSession = {
    //读取配置文件
    val filePath = System.getProperty("user.dir")
    val properties: Properties = new Properties()
    val ipstream = new BufferedInputStream(new FileInputStream(filePath + "/conf/configkmean.properties"))
    properties.load(ipstream)

    val masterUrl = properties.getProperty("spark.master.url")
    val appName = properties.getProperty("spark.app.name")
    val sparkconf = new SparkConf()
      .setMaster(masterUrl)
      .setAppName(appName)
      .set("spark.port.maxRetries", "100")
    val Spark = SparkSession.builder().config(sparkconf).getOrCreate()
    //    val Spark = SparkSession
    //      .builder()
    //      .master(masterUrl)
    //      .appName(appName)
    //      .config("spark.some.config.option", "some-value")
    //      .set("","")
    //      .getOrCreate()

    Spark
  }

  //生成时间
  def getNowTime(): String = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time = new Date().getTime
    format.format(time)
  }

  //main函数
  def main(args: Array[String]) {
    var i = 0
    val allmainstarttime = (new Date().getTime + "").substring(0, 10).toLong
    logger.error("程序开始运行" + allmainstarttime.toString + "获取IPS")
    //val IPS = getIPList()
    val IPS = List("221.176.64.146", "221.176.64.166")
    val JUDGE = getIPList()//获取ip的种类
    val getipsendtime = (new Date().getTime + "").substring(0, 10).toLong
    logger.error("获取IPS结束，花费时间：" + (getipsendtime - allmainstarttime).toString)
    val len = IPS.length
    //计算时间
    val CLTIME = getNowTime

    val kMeansModelOne: kMeansModelOne = new kMeansModelOne() //一级聚类
    val ReturnCenter = new ReturnCenter()
    val HbaseProcessSingle = new HbaseProcessSingle()
    //遍历每个ip IPSList("221.176.64.146", "221.176.64.166")
    for (ip <- IPS) { //将221.176.64.146", "221.176.64.166的记录导出来
      i = i + 1
      logger.error(ip.toString + i.toString + (len - i).toString)
                          //ip+i+len-i
      //只有包含才进行计算
      if (JUDGE.contains(ip)) {

        //netflowdata
        val dataDF: DataFrame = getDataOutNetflow(ip).toDF()
        logger.error("netflow原始数据")
        dataDF.show(10)
        val (kmeansTrainOne, predictionnum) = kMeansModelOne.kMeansModel(spark, dataDF, 10)
        logger.error("计算完成")
        //kmeansTrainOne.show()

        //生成计算批次
        val CLNO = UUID.randomUUID().toString

        if (predictionnum.length != 0) {
          val ResultDF: DataFrame = ReturnCenter.ReturnCenterOne(spark, kmeansTrainOne, predictionnum)
          //TYPE
          val TYPE = "1"
          //中心点的DEMO
          val DEMOCENTER = "proto#time#packnum#bytesize"
          //簇点的DEMO
          val DEMOPOINT = "proto#method#time#packnum#bytesize#recordtime"
          HbaseProcessSingle.insertSIEMCenter(ResultDF, CLNO, CLTIME, TYPE, DEMOCENTER, spark, properties)
          HbaseProcessSingle.insertSIEMCluster(ResultDF, CLNO, CLTIME, DEMOPOINT, spark, properties)

          //    monidata
          val monidataDF: DataFrame = getDataOutMoni(ip)
          logger.error("moni原始数据")
          //monidataDF.show(10)

          val kMeansModelTwo: kMeansModelTwo = new kMeansModelTwo()
          kMeansModelTwo.kMeansModel(spark, kmeansTrainOne, monidataDF, 10, predictionnum, CLNO, CLTIME, properties)

        }
      }
    }

    val allmainendtime = (new Date().getTime + "").substring(0, 10).toLong
    logger.error("全部程序运行结束：" + (allmainendtime - allmainstarttime).toString)
  }

  //读取数据
  def getDataOutNetflow(ip: String): DataFrame = {
    //val netflowpathIP = "hdfs://172.16.12.38:9000/spark/netflowHDFS/2017-04-13"
    //val netflowpathIP = "E:/ScalaCode/MODEL/src/main/scala/scode/AssetsPortrait/data1.csv"
    //    val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> netflowpathIP)
    //    val netflowdataall = sql.read.options(options).format("com.databricks.spark.csv").load()
    netflowdataall.createOrReplaceTempView("BIGDATA")
    val dataDF = spark.sql(s"SELECT COSTTIME,TCP,UDP,ICMP,GRE,PACKERNUM,BYTESIZE,PROTO,FLOWD,ROW,RECORDTIME,SRCIP,DSTIP,SRCPORT,DSTPORT FROM BIGDATA WHERE DSTIP = '" + ip + "' OR SRCIP = '" + ip + "'")
      .toDF("COSTTIME", "TCP", "UDP", "ICMP", "GRE", "PACKERNUM", "BYTESIZE", "PROTO", "FLOWD", "ROW", "RECORDTIME", "SRCIP", "DSTIP", "SRCPORT", "DSTPORT")
    //转换成df
    import spark.implicits._
    val ResultDF = dataDF.rdd.map {
      row =>
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
        val IP = ip
        val SRCPORT = row.getAs[String]("SRCPORT")
        val DSTPORT = row.getAs[String]("DSTPORT")

        (COSTTIME, TCP, UDP, ICMP, GRE, PACKERNUM, BYTESIZE, PROTO, FLOWD, ROW, RECORDTIME, IP, SRCPORT, DSTPORT)
    }.toDF("COSTTIME", "TCP", "UDP", "ICMP", "GRE", "PACKERNUM", "BYTESIZE", "PROTO", "FLOWD", "ROW", "RECORDTIME", "IP", "SRCPORT", "DSTPORT")

    ResultDF
  }

  //读取数据
  def getDataOutMoni(ip: String): DataFrame = {
    //val moniflowpathIP = "hdfs://172.16.12.38:9000/spark/moniflowHDFS/2017-04-13"
    //val moniflowpathIP = "E:/ScalaCode/MODEL/src/main/scala/scode/AssetsPortrait/monidata1.csv"
    //    val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> moniflowpathIP)
    //    val monidataall = sql.read.options(options).format("com.databricks.spark.csv").load()
    moniflowdataall.createOrReplaceTempView("BIGDATA")
    val dataDF = spark.sql(s"SELECT HTTP,QQ,FTP,TELNET,DBAUDIT,BDLOCALTROJAN,APPPROTO,GET,POST,METHOD," +
      s"SRCIP,DSTIP,SRCPORT,DSTPORT,HOST,URI,ROW,DATE FROM BIGDATA WHERE DSTIP = '" + ip + "' OR SRCIP = '" + ip + "'")
      .toDF("HTTP", "QQ", "FTP", "TELNET", "DBAUDIT", "BDLOCALTROJAN", "APPPROTO", "GET", "POST", "METHOD",
        "SRCIP", "DSTIP", "SRCPORT", "DSTPORT", "HOST", "URI", "ROW", "DATE")

    import spark.implicits._
    val ResultDF = dataDF.rdd.map {
      row =>
        val HTTP = row.getAs[String]("HTTP")
        val QQ = row.getAs[String]("QQ")
        val FTP = row.getAs[String]("FTP")
        val TELNET = row.getAs[String]("TELNET")
        val DBAUDIT = row.getAs[String]("DBAUDIT")
        val BDLOCALTROJAN = row.getAs[String]("BDLOCALTROJAN")
        val APPPROTO = row.getAs[String]("APPPROTO")
        val GET = row.getAs[String]("GET")
        val POST = row.getAs[String]("POST")
        val METHOD = row.getAs[String]("METHOD")
        val IP = ip
        val SRCPORT = row.getAs[String]("SRCPORT")
        val DSTPORT = row.getAs[String]("DSTPORT")
        val HOST = row.getAs[String]("HOST")
        val URI = row.getAs[String]("URI")
        val ROW = row.getAs[String]("ROW")
        val DATE = row.getAs[String]("DATE")

        (HTTP, QQ, FTP, TELNET, DBAUDIT, BDLOCALTROJAN, APPPROTO, GET, POST, METHOD, IP, SRCPORT, DSTPORT, HOST, URI, ROW, DATE)
    }.toDF("HTTP", "QQ", "FTP", "TELNET", "DBAUDIT", "BDLOCALTROJAN", "APPPROTO", "GET", "POST", "METHOD",
      "IP", "SRCPORT", "DSTPORT", "HOST", "URI", "ROW", "DATE")

    ResultDF
  }

  //获取ip的种类
  def getIPList(): Array[String] = {
    //val netflowpathIP = "hdfs://172.16.12.38:9000/spark/netflowHDFS/2017-04-13"
    //val netflowpathIP = "E:/ScalaCode/MODEL/src/main/scala/scode/AssetsPortrait/data1.csv"
    //val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> netflowpathIP)
    //val dataall = sql.read.options(options).format("com.databricks.spark.csv").load()
    //dataall.persist(StorageLevel.MEMORY_AND_DISK)

    netflowdataall.createOrReplaceTempView("BIGDATA")
    //获取ip的种类
    val DSTIPS: Array[String] = spark.sql(s"SELECT DISTINCT SRCIP FROM BIGDATA WHERE FLOWD='0' OR FLOWD='2'").rdd.map(_.toSeq.toList.head.toString).collect()
    val SRCIPS: Array[String] = spark.sql(s"SELECT DISTINCT DSTIP FROM BIGDATA WHERE FLOWD='1' OR FLOWD='2'").rdd.map(_.toSeq.toList.head.toString).collect()
    //合并数组
    val IPS: Array[String] = (DSTIPS ++ SRCIPS).distinct
    IPS  //返回ip
  }

}

/**
  * 一级KMEANS聚类
  */

//一级聚类的类
class kMeansModelOne {
  val logger = Logger.getLogger(this.getClass)

  //转换数据类型
  def dataInit(spark: SparkSession, dataDF: DataFrame): DataFrame = {
    import spark.implicits._
    val dataST = dataDF.rdd.map {
      line =>
        var ll = line.toSeq.toArray.map(_.toString)
        var ll2 = ll.drop(7)
        (Vectors.dense(ll.take(7).map(_.toDouble)), ll2(0), ll2(1), ll2(2), ll2(3), ll2(4), ll2(5), ll2(6))
    }.toDF("FEATURES", "PROTO", "FLOWD", "ROW", "RECORDTIME", "IP", "SRCPORT", "DSTPORT")
    logger.error("转换Vector类型")
    //dataST.show(3)
    dataST
  }

  //聚类主函数
  def kMeansModel(spark: SparkSession, dataDF: DataFrame, itNum: Int): (DataFrame, Array[Int]) = {
    //归一化处理
    val dataST: DataFrame = dataInit(spark, dataDF)
    val scaler = new MinMaxScaler().setInputCol("FEATURES").setOutputCol("SCLFEATURES").fit(dataST)
    val sclData: DataFrame = scaler.transform(dataST)
    logger.error("netflow归一化处理")
    //sclData.show(5)

    //定义sql和sc
    val sql: SQLContext = spark.sqlContext

    //判断输入的数据量有多少
    val total_rows: Int = sclData.count().toInt
    var k = total_rows
    if (total_rows > itNum) {
      k = itNum
    }
    else if (total_rows == itNum) {
      k = itNum - 1
    }
    else if (total_rows <= 2) {
      k = -1
    }
    else {
      k = total_rows - 1
    }

    //聚类
    //val silhouetteCoefficient = new silhouetteCoefficient()
    //求最优的K值
    var numClusters = k
    if (k == -1) {
      numClusters = 2
    }
    else {
      //      numClusters = silhouetteCoefficient.silhouetteCoefficient(sclData, k, 3, sql, sc)
      numClusters = 3
    }
    val Iterstarttime = (new Date().getTime + "").substring(0, 10).toLong
    logger.error("1numClusters" + numClusters.toString + Iterstarttime.toString)
    //迭代次数100次写死
    val numIterations: Int = 100
    val kmeans = new KMeans().setK(numClusters).setMaxIter(numIterations)
    val kMeansModel: KMeansModel = kmeans.setFeaturesCol("SCLFEATURES").fit(sclData)

    //kMeansModel.save("hdfs://172.16.12.38:9000/spark/kmeansmodeltao")

    //簇中心和簇点
    val clusterCenters: Array[Vector] = kMeansModel.clusterCenters
    val centersList = clusterCenters.toList
    val centersListBD = spark.sparkContext.broadcast(centersList)

    val Iterendtime = (new Date().getTime + "").substring(0, 10).toLong
    logger.error("IterEnd:" + (Iterendtime - Iterstarttime).toString)

    //筛选得到样本大于2的非离群点
    kMeansModel.transform(sclData).createOrReplaceTempView("BIGDATA")
    val PREDICTIONNUM: Array[Int] = sql.sql(s"SELECT prediction FROM BIGDATA GROUP BY prediction HAVING COUNT(*)>2").select("prediction").rdd.map(r => r(0).toString.toDouble.toInt).collect()

    import spark.implicits._
    val kmeansTrainOne: DataFrame = kMeansModel.transform(sclData).rdd.map {
      row =>
        val centersListBDs = centersListBD.value
        val FEATURES = row.getAs[Vector]("FEATURES")
        val PROTO = row.getAs[String]("PROTO")
        val FLOWD = row.getAs[String]("FLOWD")
        val ROW = row.getAs[String]("ROW")
        val RECORDTIME = row.getAs[String]("RECORDTIME")
        val IP = row.getAs[String]("IP")
        val SRCPORT = row.getAs[String]("SRCPORT")
        val DSTPORT = row.getAs[String]("DSTPORT")
        val SCLFEATURES = row.getAs[Vector]("SCLFEATURES")
        val PREDICTION = row.getAs[Int]("prediction")
        val centerPoint: Vector = centersListBDs(PREDICTION)
        //如果包含样本数大于2，则记为prediction，样本数<=2则记为OTHER
        val TEMP: Int = PREDICTION
        var PREDICTIONS: String = ""
        if (PREDICTIONNUM.contains(TEMP) == true) {
          PREDICTIONS = PREDICTION.toString
        }
        else {
          PREDICTIONS = "other"
        }
        //距离
        val DISTANCE = math.sqrt(centerPoint.toArray.zip(SCLFEATURES.toArray).
          map(p => p._1 - p._2).map(d => d * d).sum)

        (FEATURES, PROTO, FLOWD, ROW, RECORDTIME, IP, SRCPORT, DSTPORT, SCLFEATURES, PREDICTIONS, centerPoint, DISTANCE)
    }.toDF("FEATURES", "PROTO", "FLOWD", "ROW", "RECORDTIME", "IP",
      "SRCPORT", "DSTPORT", "SCLFEATURES", "PREDICTION", "CENTERPOINT", "DISTANCE")
    logger.error("重新构造datarame结束")
    (kmeansTrainOne, PREDICTIONNUM)

  }
}

/**
  * 二级KMEANS聚类
  */

//二级聚类的类
class kMeansModelTwo extends Serializable {
  @transient
  val logger = Logger.getLogger(this.getClass)

  //匹配数据
  def MatchData(spark: SparkSession, dataDF: DataFrame, monidataDF: DataFrame, prediction: Int): DataFrame = {
    //创建表名
    //    val sc = spark.sparkContext
    //    val sqlContext = new SQLContext(sc)
    dataDF.createOrReplaceTempView("NETFLOW")
    monidataDF.createOrReplaceTempView("MONI")
    val NewdataDF: DataFrame = spark.sql(
      "SELECT MONI.HTTP,MONI.QQ,MONI.FTP,MONI.TELNET,MONI.DBAUDIT,MONI.BDLOCALTROJAN,MONI.APPPROTO,MONI.GET,MONI.POST,MONI.METHOD,MONI.IP,MONI.SRCPORT,MONI.DSTPORT,MONI.HOST,MONI.URI,MONI.ROW,MONI.DATE,NETFLOW.PREDICTION " +
        "FROM NETFLOW,MONI " +
        "WHERE NETFLOW.PREDICTION='" + prediction.toString + "' " +
        "AND NETFLOW.IP=MONI.IP AND NETFLOW.SRCPORT=MONI.SRCPORT AND NETFLOW.DSTPORT=MONI.DSTPORT") // AND NETFLOW.RECORDTIME=MONI.DATE AND (NETFLOW.PREDICTION<>1)
    logger.error("sql查询" + prediction.toString)
    //NewdataDF.show()
    NewdataDF
  }

  //转换数据类型
  def ttoString(col: Any) = {
    if (col == null) "0" else col.toString
  }

  def dataInit(spark: SparkSession, NewdataDF: DataFrame): DataFrame = {
    import spark.implicits._
    val dataST = NewdataDF.rdd.map {
      line =>
        var ll = line.toSeq.toArray.map(ttoString(_))
        var ll2 = ll.drop(6)
        (Vectors.dense(ll.take(6).map(_.toDouble)), ll2(0), ll2(1), ll2(2), ll2(3), ll2(4), ll2(5), ll2(6), ll2(7), ll2(8), ll2(9), ll2(10), ll2(11))
    }.toDF("FEATURES", "APPPROTO", "GET", "POST", "METHOD", "IP", "SRCPORT", "DSTPORT", "HOST", "URI", "ROW", "DATE", "FATHERPREDICTION")
    logger.error("转换Vector类型")
    //dataST.show(12)
    dataST
  }

  //聚类主函数
  def kMeansModel(spark: SparkSession, dataDF: DataFrame, monidataDF: DataFrame, itNum: Int, predictionnum: Array[Int], CLNO_P: String, CLTIME: String, properties: Properties) {

    //循环查询出不同类别的数据
    for (prediction <- predictionnum) {
      breakable {
        //这里写与上网行为的数据匹配
        var NewdataDF: DataFrame = MatchData(spark: SparkSession, dataDF: DataFrame, monidataDF: DataFrame, prediction: Int)

        if (NewdataDF.count().toInt == 0) {
          break()
        }

        //归一化处理
        val dataST: DataFrame = dataInit(spark, NewdataDF)
        val scaler = new MinMaxScaler().setInputCol("FEATURES").setOutputCol("SCLFEATURES").fit(dataST)
        val sclData: DataFrame = scaler.transform(dataST)
        logger.error("moni归一化处理")
        //sclData.show(5)

        //定义sql和sc
        //        val sql: SQLContext = spark.sqlContext
        //        val sc: SparkContext = spark.sparkContext

        //判断输入的数据量有多少
        val total_rows: Int = sclData.count().toInt
        var k = total_rows - 1
        if (total_rows > itNum) {
          k = itNum
        }
        else if (total_rows == itNum) {
          k = itNum - 1
        }
        else if (total_rows <= 2) {
          //<=2的时候是根本无法聚类的，所以直接传错误参数下去
          k = -1
        }
        else {
          k = total_rows - 1
        }

        //聚类
        //val silhouetteCoefficient = new silhouetteCoefficient()
        //判断k是否是错误参数
        var numClusters = k
        //如果k<=2就直接拼接结果
        if (k == -1) {
          numClusters = 2
        }
        //如果k>2就聚类
        else {
          //求最优的K值
          //          numClusters = silhouetteCoefficient.silhouetteCoefficient(sclData, k, 3, sql, sc)
          k = 3
        }
        val Iterstarttime = (new Date().getTime + "").substring(0, 10).toLong
        logger.error("2numClusters" + (numClusters, Iterstarttime).toString())
        //迭代次数100次写死
        val numIterations: Int = 100
        var kmeans = new KMeans().setK(numClusters).setMaxIter(numIterations)
        var kMeansModel: KMeansModel = kmeans.setFeaturesCol("SCLFEATURES").fit(sclData)

        val Iterendtime = (new Date().getTime + "").substring(0, 10).toLong
        logger.error("2IterEnd:" + (Iterendtime - Iterstarttime).toString)

        //簇中心和簇点
        val clusterCenters = kMeansModel.clusterCenters
        val centersList = clusterCenters.toList
        val centersListBD = spark.sparkContext.broadcast(centersList)
        import spark.implicits._
        val kmeansTrainTwo: DataFrame = kMeansModel.transform(sclData).rdd.map {
          row =>
            val centersListBDs = centersListBD.value
            val FEATURES = row.getAs[Vector]("FEATURES")
            val APPPROTO = row.getAs[String]("APPPROTO")
            val GET = row.getAs[String]("GET")
            val POST = row.getAs[String]("POST")
            val METHOD = row.getAs[String]("METHOD")
            val IP = row.getAs[String]("IP")
            val SRCPORT = row.getAs[String]("SRCPORT")
            val DSTPORT = row.getAs[String]("DSTPORT")
            val HOST = row.getAs[String]("HOST")
            val URI = row.getAs[String]("URI")
            val ROW = row.getAs[String]("ROW")
            val DATE = row.getAs[String]("DATE")
            val SCLFEATURES = row.getAs[Vector]("SCLFEATURES")
            val FATHERPREDICTION = row.getAs[String]("FATHERPREDICTION")
            val PREDICTION = row.getAs[Int]("prediction")
            val centerPoint: Vector = centersListBDs(PREDICTION)
            //距离
            val DISTANCE = math.sqrt(centerPoint.toArray.zip(SCLFEATURES.toArray).
              map(p => p._1 - p._2).map(d => d * d).sum)

            (FEATURES, APPPROTO, GET, POST, METHOD, IP, SRCPORT, DSTPORT, HOST, URI, ROW, DATE, SCLFEATURES, FATHERPREDICTION, PREDICTION, centerPoint, DISTANCE)
        }.toDF("FEATURES", "APPPROTO", "GET", "POST", "METHOD", "IP", "SRCPORT", "DSTPORT",
          "HOST", "URI", "ROW", "DATE", "SCLFEATURES", "FATHERPREDICTION", "PREDICTION", "CENTERPOINT", "DISTANCE")

        //println("k>2")
        //kmeansTrainTwo.show(10)

        //创建表名
        kmeansTrainTwo.createOrReplaceTempView("BIGDATA")
        val predictionnum: Array[Int] = spark.sql(s"SELECT DISTINCT PREDICTION FROM BIGDATA").select("PREDICTION").rdd.map(r => r(0).toString.toDouble.toInt).collect()
        val ReturnCenter = new ReturnCenter()
        if (predictionnum.length != 0) {
          logger.error("二次聚类中心点")
          val ResultDF = ReturnCenter.ReturnCenterTwo(spark, kmeansTrainTwo, predictionnum)
          val HbaseProcessSingle = new HbaseProcessSingle()
          //生成计算批次
          val CLNO = UUID.randomUUID().toString
          //TYPE
          val TYPE = "2"
          //中心点的DEMO
          val DEMOCENTER = "proto"
          //簇点的DEMO
          val DEMOPOINT = "proto#time#packnum#bytesize#recordtime"
          //二级聚类插入中心点
          HbaseProcessSingle.insertSIEMCenterSec(ResultDF, CLNO, CLNO_P, CLTIME, TYPE, DEMOCENTER, spark, properties)
          //二级聚类插入簇点
          HbaseProcessSingle.insertSIEMCluster(ResultDF, CLNO, CLTIME, DEMOPOINT, spark, properties)
        }
      }
    }
  }
}


/**
  * 轮廓系数
  */
//class silhouetteCoefficient {
//  //轮廓系数
//  def silhouetteCoefficient(sclData: DataFrame, k: Int, iteration: Int = 3, sql: SQLContext, sc: SparkContext) = {
//    //计算轮廓系数
//    def shcoefficient(a: Double, b: Double): Double = {
//      if (Math.max(a, b) == 0.0) {
//        0.0
//      } else {
//        (b - a) / Math.max(a, b)
//      }
//    }
//
//    //欧式距离
//    def edulideanDist(point: Array[Double], center: Array[Double]) = {
//      var distance: Double = 0.0
//      try {
//        distance = Math.sqrt(point.zip(center).map {
//          case (x: Double, y: Double) => Math.pow(y - x, 2)
//        }.sum
//        )
//      } catch {
//        case e: Exception => {
//          println(s"[AssetPortrait] Some error occur during getEucDistance !!: " + e.getMessage)
//        }
//      }
//      distance
//    }
//
//    //遍历k次进行kmeans
//    var shcoffAll: Map[Int, Double] = Map()
//    //存放所有轮廓系数
//    val kit: List[Int] = (2 to k).toList
//    //遍历k
//    kit.foreach { k =>
//      println("当前k：" + k + "\t========================================================")
//      var shcoffEach: ArrayBuffer[Double] = ArrayBuffer[Double]()
//      //存放特定k下每个点的轮廓系数
//      //迭代iteration次
//      val iterit: List[Int] = (1 to iteration).toList
//      iterit.foreach { iter =>
//        println("当前迭代次数：" + iter + "\t******************************")
//        val starttime = getNowTime()
//        println("start time:" + starttime)
//        var kmeans = new KMeans().setK(k)
//        //.setSeed(1L)
//        var kMeansModel: KMeansModel = kmeans.setFeaturesCol("SCLFEATURES").fit(sclData)
//        val kmeansmodel: KMeansModel = new KMeans().setK(k).setFeaturesCol("SCLFEATURES").setPredictionCol("LABEL").fit(sclData)
//        //.setMaxIter(20)
//        //类中心点坐标
//        val result: DataFrame = kmeansmodel.transform(sclData)
//        //        println("轮廓系数聚类：")//
//        //        result.show()
//        result.persist(StorageLevel.MEMORY_AND_DISK_SER)
//        //        val fs = sc.broadcast(result)//设置广播变量
//
//        //遍历簇类别
//        val indexs = result.select("LABEL").distinct().rdd.map(row => row(0).toString().toInt).collect()
//        //        println("共有的簇类别："+indexs.toList)//
//        var shcoff: ArrayBuffer[Double] = ArrayBuffer[Double]() //存放轮廓系数列表
//        indexs.foreach { index =>
//          //          println(s"当前簇:"+index)//
//          //遍历每个点
//          //          val result = fs.value//获取广播变量
//          result.filter(s"LABEL='$index'").select("SCLFEATURES").collect().map { row =>
//            var rec = Vectors.dense(Array(0.0, 0.0))
//            row(0) match {
//              case rowvec: Vector => rec = rowvec
//              case _ => 0
//            }
//            rec.toArray
//          }.foreach { line =>
//            //            println("当前簇包含的数据："+line.toList)
//            //            val result = fs.value//获取广播变量
//            //遍历同簇内所有点
//            var avgTemp: ArrayBuffer[Double] = ArrayBuffer[Double]() //存放avg列表
//            //            result.filter(s"LABEL='$index'").select("SCLFEATURES").rdd.map { row =>
//            //              var rec=Vectors.dense(Array(0.0,0.0));row(0) match {case rowvec:Vector=>rec=rowvec case _=> 0};rec.toArray.toList}.collect().foreach(println)
//            result.filter(s"LABEL='$index'").select("SCLFEATURES").collect().map { row =>
//              var rec = Vectors.dense(Array(0.0, 0.0))
//              row(0) match {
//                case rowvec: Vector => rec = rowvec
//                case _ => 0
//              }
//              rec.toArray
//            }.foreach { lineAvg =>
//              //              println("簇内其他点的数据："+lineAvg.toList)
//              var pointDist1 = edulideanDist(lineAvg, line) //计算距离
//              avgTemp += pointDist1
//            }
//            //avg
//            //            println("簇内所有点的距离："+avgTemp.toList)//
//            var aTemp: Double = avgTemp.toArray.sum / avgTemp.toArray.count(_ >= 0)
//            //avg
//            //            println("当前a(i)：" + aTemp)//
//            //遍历不同簇内所有点
//            //
//            var minTemp: ArrayBuffer[Double] = ArrayBuffer[Double]()
//            //存放min列表
//            val otherClu = result.select("LABEL").distinct().rdd.map(row => row(0).toString.toInt).filter(n => n != index).collect() //统计其他簇类别标签
//            //            println("组外簇列表："+otherClu.toList)//
//            otherClu.foreach { eachClu =>
//              //              println("当前组外簇："+eachClu)//
//              var minTemp1: ArrayBuffer[Double] = ArrayBuffer[Double]()
//              result.filter(s"LABEL='$eachClu'").select("SCLFEATURES").collect().map { row =>
//                var rec = Vectors.dense(Array(0.0, 0.0))
//                row(0) match {
//                  case rowvec: Vector => rec = rowvec
//                  case _ => 0
//                }
//                rec.toArray
//              }.foreach { lineMin =>
//                //                println("当前组外簇包含的数据："+lineMin.toList)
//                var pointDist2 = edulideanDist(lineMin, line)
//                minTemp1 += pointDist2
//              }
//              //min()里面的
//              var bavgTemp: Double = minTemp1.sum / minTemp1.count(_ > 0) //min()里面的平均
//              minTemp += bavgTemp
//            }
//            //other clusters
//            val bTemp = minTemp.toArray.min
//            //            println("当前b(i)：" + bTemp)//
//
//            //轮廓系数
//            shcoff += shcoefficient(aTemp, bTemp)
//          } //每个点
//        } //每个簇
//        shcoffEach += shcoff.toArray.sum / shcoff.toArray.count(_ > 0)
//        val endtime = getNowTime()
//        println("end time:" + endtime)
//      } //迭代
//      println("si hou coff:" + shcoffEach.toArray.sum / shcoffEach.count(_ > 0))
//      shcoffAll += (k -> shcoffEach.toArray.sum / shcoffEach.count(_ > 0))
//    }
//    //k
//    var shTemp: Double = 0.0
//    var bestK: Int = 0
//    shcoffAll.keys.foreach { key =>
//      if (shcoffAll(key) > shTemp) {
//        shTemp = shcoffAll(key)
//        bestK = key
//      }
//      if (shcoffAll(key) == shTemp) {
//        if (bestK > key) {
//          bestK = key
//        }
//      }
//
//      println("k:" + key + "------")
//      println("sihou coff:" + shcoffAll(key))
//    }
//    println("best k:" + bestK)
//    bestK
//  }
//
//  def getNowTime(): String = {
//    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val time = new Date().getTime
//    format.format(time)
//  }
//}


/**
  * 返回中心点原始数据
  */
class ReturnCenter extends Serializable {
  @transient
  val logger = Logger.getLogger(this.getClass)

  //一级聚类返回中心点坐标
  def ReturnCenterOne(spark: SparkSession, kMeansOneDF: DataFrame, predictionnum: Array[Int]): DataFrame = {
    //定义一个空的map
    var CenterMap = mutable.Map.empty[Int, List[Double]]

    //创建表名
    //    @transient
    //    val sql: SQLContext = spark.sqlContext
    kMeansOneDF.createOrReplaceTempView("BIGDATA")
    for (label <- predictionnum) {
      //簇点长度
      var FeaturesLen: Int = spark.sql("SELECT COUNT(*) FROM BIGDATA WHERE PREDICTION='" + label.toString + "'").collect().head.getLong(0).toInt
      //计算中心坐标
      var FeaturesList: List[Double] = spark.sql("SELECT FEATURES FROM BIGDATA WHERE PREDICTION='" + label.toString + "'").select("FEATURES").rdd
        .map {
          line =>
            var tmp: List[Double] = Nil
            line(0) match {
              case vec: org.apache.spark.ml.linalg.Vector => tmp = vec.toArray.toList
              case _ => 0
            }
            tmp
        }.reduce {
        (x, y) =>
          x.zip(y).map(p => (p._1 + p._2))
      }.map(p => p / FeaturesLen)
      //构造成一个map
      CenterMap += (label -> FeaturesList)
    }

    import spark.implicits._
    val ResultDF = kMeansOneDF.rdd.map {
      row =>
        val FEATURES = row.getAs[Vector]("FEATURES")
        val PROTO = row.getAs[String]("PROTO")
        val FLOWD = row.getAs[String]("FLOWD")
        val ROW = row.getAs[String]("ROW")
        val RECORDTIME = row.getAs[String]("RECORDTIME")
        val IP = row.getAs[String]("IP")
        val SRCPORT = row.getAs[String]("SRCPORT")
        val DSTPORT = row.getAs[String]("DSTPORT")
        val SCLFEATURES = row.getAs[Vector]("SCLFEATURES")
        val PREDICTION = row.getAs[String]("PREDICTION")
        val CENTERPOINT = row.getAs[Vector]("CENTERPOINT")
        val DISTANCE = row.getAs[Double]("DISTANCE").toDouble
        var RETURNCENTER: List[String] = Nil
        //反归一化的中心点
        if (PREDICTION == "other") {
          RETURNCENTER = RETURNCENTER :+ (-1).toString
        } else {
          RETURNCENTER = ReturnProto(CenterMap(PREDICTION.toDouble.toInt), 1, 4)
        }

        (FEATURES, PROTO, FLOWD, ROW, RECORDTIME, IP, SRCPORT, DSTPORT, SCLFEATURES, PREDICTION, CENTERPOINT, DISTANCE, RETURNCENTER)
    }.toDF("FEATURES", "PROTO", "FLOWD", "ROW", "RECORDTIME", "IP", "SRCPORT", "DSTPORT",
      "SCLFEATURES", "PREDICTION", "CENTERPOINT", "DISTANCE", "RETURNCENTER")

    //ResultDF.show(10)
    ResultDF
  }

  //二级聚类返回中心点坐标
  def ReturnCenterTwo(spark: SparkSession, kMeansTwoDF: DataFrame, predictionnum: Array[Int]): DataFrame = {
    //定义一个空的map
    var CenterMap = mutable.Map.empty[Int, List[Double]]
    for (label <- predictionnum) {
      //簇点长度
      var FeaturesLen: Int = spark.sql("SELECT COUNT(*) FROM BIGDATA WHERE PREDICTION='" + label.toString + "'").collect().head.getLong(0).toInt
      //计算中心坐标
      var FeaturesList: List[Double] = spark.sql("SELECT FEATURES FROM BIGDATA WHERE PREDICTION='" + label.toString + "'").select("FEATURES").rdd
        .map {
          line =>
            var tmp: List[Double] = Nil
            line(0) match {
              case vec: org.apache.spark.ml.linalg.Vector => tmp = vec.toArray.toList
              case _ => 0
            }
            tmp
        }.reduce {
        (x, y) =>
          x.zip(y).map(p => (p._1 + p._2))
      }.map(p => p / FeaturesLen)
      //构造成一个map
      CenterMap += (label -> FeaturesList)
    }

    import spark.implicits._
    val ResultDF = kMeansTwoDF.rdd.map {
      row =>
        val FEATURES = row.getAs[Vector]("FEATURES")
        val APPPROTO = row.getAs[String]("APPPROTO")
        val GET = row.getAs[String]("GET")
        val POST = row.getAs[String]("POST")
        val METHOD = row.getAs[String]("METHOD")
        val IP = row.getAs[String]("IP")
        val SRCPORT = row.getAs[String]("SRCPORT")
        val DSTPORT = row.getAs[String]("DSTPORT")
        val HOST = row.getAs[String]("HOST")
        val URI = row.getAs[String]("URI")
        val ROW = row.getAs[String]("ROW")
        val DATE = row.getAs[String]("DATE")
        val SCLFEATURES = row.getAs[Vector]("SCLFEATURES")
        val FATHERPREDICTION = row.getAs[String]("FATHERPREDICTION")
        val PREDICTION = row.getAs[Int]("PREDICTION")
        val CENTERPOINT = row.getAs[Vector]("CENTERPOINT")
        val DISTANCE = row.getAs[Double]("DISTANCE").toDouble
        //反归一化的中心点
        var RETURNCENTER: List[String] = ReturnProto(CenterMap(PREDICTION.toDouble.toInt), 0, 6)

        (FEATURES, APPPROTO, GET, POST, METHOD, IP, SRCPORT, DSTPORT, HOST, URI, ROW, DATE, SCLFEATURES, FATHERPREDICTION, PREDICTION, CENTERPOINT, DISTANCE, RETURNCENTER)
    }.toDF("FEATURES", "APPPROTO", "GET", "POST", "METHOD", "IP", "SRCPORT", "DSTPORT",
      "HOST", "URI", "ROW", "DATE", "SCLFEATURES", "FATHERPREDICTION", "PREDICTION", "CENTERPOINT", "DISTANCE", "RETURNCENTER")
    logger.error("二级反归一化结果")
    //ResultDF.show(10)
    ResultDF
  }

  //返回协议
  //传入列表、开始位置、协议长度
  def ReturnProto(centerlist: List[Double], startposition: Int, lengths: Int): List[String] = {

    var newlist: List[String] = List()
    //如果长度为4，则是一级聚类返回协议
    if (lengths == 4) {
      var values: Double = 0.0
      val protolist: List[String] = List("TCP", "UDP", "ICMP", "GRE")
      var proto: String = ""
      for (i <- startposition to lengths) {
        if (centerlist(i) > values) {
          values = centerlist(i)
          proto = protolist(i - startposition)
        }
      }

      for (j <- 0 to centerlist.length - 1) {
        if (j == 1) {}
        else if (j == 2) {}
        else if (j == 3) {}
        else if (j == 4) {}
        else if (j == 0) {
          newlist = newlist :+ proto
          newlist = newlist :+ centerlist(j).toInt.toString
        }
        else {
          newlist = newlist :+ centerlist(j).toInt.toString
        }
      }
    }
    //如果长度为6，则是二级聚类返回协议
    else if (lengths == 6) {
      var values: Double = 0.0
      val protolist: List[String] = List("HTTP", "QQ", "FTP", "TELNET", "DBAUDIT", "BDLOCALTROJAN")
      var proto: String = ""
      for (i <- startposition to lengths - 1) {
        if (centerlist(i) > values) {
          values = centerlist(i)
          proto = protolist(i - startposition)
        }
      }
      newlist = newlist :+ proto
    }
    newlist
  }
}


import java.sql.Timestamp

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.DBUtils

/**
  * 存入数据库的类
  */
class HbaseProcessSingle extends Serializable {
  @transient
  val logger = Logger.getLogger(this.getClass)

  //一级聚类插入簇中心点表
  def insertSIEMCenter(data: DataFrame, CLNO: String, TIME: String, TYPE: String, DEMO: String, spark: SparkSession, properties: Properties) = {
    //val conn = new DBUtils().getPhoniexConnect("10.252.47.211,10.252.47.212,10.252.47.213,10.252.47.215:2181") //"10.252.47.211,10.252.47.212,10.252.47.213,10.252.47.215:2181"
    val conn = new DBUtils().getPhoniexConnect(properties.getProperty("zookeeper")) //"10.252.47.211,10.252.47.212,10.252.47.213,10.252.47.215:2181"
    conn.setAutoCommit(false)
    val state = conn.createStatement()
    //获取中心点的数据
    data.createOrReplaceTempView("OriData")
    val result = spark.sql("select distinct IP,PREDICTION,RETURNCENTER from OriData").toDF()
    try {
      val tempResult = result.rdd.map { row =>
        val recordid: String = ROWUtils.genaralROW()
        val index: String = row.getAs[String]("PREDICTION")
        val info: String = row.getAs[List[Int]]("RETURNCENTER").mkString("#")
        val clno: String = CLNO.replace("-", "")
        val Type: String = TYPE
        val cltime: String = TIME
        val demo: String = DEMO
        val name: String = row.getAs[String]("IP")
        //s"TCP#UDP#ICMP#GRE#tftime#packernum#bytesize"
        val dataori: String = "s"
        List(recordid, index, info, clno, Type, cltime, demo, dataori, name)
      }
      val lastdata: Array[List[String]] = tempResult.collect()
      logger.error("一级聚类，插入hbase的示例数据:")
      //lastdata.take(10).foreach(println)
      for (row <- lastdata) {
        val sql = new StringBuilder()
          .append("upsert into T_SIEM_KMEANS_CENTER (\"ROW\",\"CENTER_INDEX\",\"CENTER_INFO\",\"CLNO\",\"TYPE\",\"CLTIME\",\"DEMO\",\"DATA_ORIGIN\",\"NAME\") ")
          .append("values ('" + row(0) + "','" + row(1) + "','" + row(2) + "','" + row(3) + "'," + row(4) + ",'" + Timestamp.valueOf(row(5)) + "','" + row(6) + "','" + row(7) + "','" + row(8) + "')")
          .toString()
        //          .append("values ('" + recordid + "','" + index + "','" + info + "','" + clno + "'," + Type + ",'"  + Timestamp.valueOf(cltime) +"','" + demo+"','" + dataori + "')" )
        state.execute(sql)
        //conn.commit()
      }
      logger.error(s"insert into hbase success!")
    } catch {
      case e: Exception => {
        logger.error(s"Insert into Hbase occur error!! " + e.getMessage)
        conn.rollback()
      }
    } finally {
      state.close()
      conn.close()
    }
  }


  //聚类簇点插入到hbase
  def insertSIEMCluster(data: DataFrame, CLNO: String, TIME: String, DEMO: String, spark: SparkSession, properties: Properties) = {
    //val conn = new DBUtils().getPhoniexConnect("10.252.47.211,10.252.47.212,10.252.47.213,10.252.47.215:2181") //"10.252.47.211,10.252.47.212,10.252.47.213,10.252.47.215:2181"
    val conn = new DBUtils().getPhoniexConnect(properties.getProperty("zookeeper")) //"10.252.47.211,10.252.47.212,10.252.47.213,10.252.47.215:2181"
    conn.setAutoCommit(false)
    val state = conn.createStatement()
    //获取中心点的数据

    try {
      val result = data.rdd.map { row =>
        val recordid: String = ROWUtils.genaralROW()
        val name: String = row.getAs[String]("IP")
        var index: String = ""
        val clno: String = CLNO.replace("-", "")
        val distance: String = row.getAs[Double]("DISTANCE").toString
        val FEATURESLIST: List[Double] = row.getAs[Vector]("FEATURES").toArray.toList
        var info: String = ""
        //如果长度为6则是上网行为的FEATURES
        if (FEATURESLIST.length == 6) {
          //得到方法
          val method: String = row.getAs[String]("METHOD")
          info = ReturnProto(FEATURESLIST, 0, 6)(0).toString() + "#" + method
          index = row.getAs[Int]("PREDICTION").toString
        } else {
          val recordtime = row.getAs[String]("RECORDTIME")
          info = ReturnProto(FEATURESLIST, 1, 4).mkString("#") + "#" + recordtime
          index = row.getAs[String]("PREDICTION")
        }
        val cltime: String = TIME
        val demo: String = DEMO
        //s"TCP#UDP#ICMP#GRE#tftime#packernum#bytesize"
        val dataori: String = "s"
        List(recordid, clno, name, index, distance, info, cltime, demo, dataori)
      }
      val lastdata: Array[List[String]] = result.collect()
      logger.error("聚类后簇点，插入hbase的示例数据:")
      //lastdata.take(10).foreach(println)
      for (row <- lastdata) {
        val sql = new StringBuilder()
          .append("upsert into T_SIEM_KMEANS_CLUSTER (\"ROW\",\"CLNO\",\"NAME\",\"CENTER_INDEX\",\"DISTANCE\",\"CLUSTER_INFO\",\"CLTIME\",\"DEMO\",\"DATA_ORIGIN\") ")
          .append("values ('" + row(0) + "','" + row(1) + "','" + row(2) + "','" + row(3) + "'," + row(4) + ",'" + row(5) + "','" + Timestamp.valueOf(row(6)) + "','" + row(7) + "','" + row(8) + "')")
          .toString()
        state.execute(sql)
        //conn.commit()
      }
      logger.error(s"insert into hbase success!")
    } catch {
      case e: Exception => {
        logger.error(s"Insert into Hbase occur error!! " + e.getMessage)
        conn.rollback()
      }
    } finally {
      state.close()
      conn.close()
    }
  }

  //二级聚类插入簇中心点表
  def insertSIEMCenterSec(data: DataFrame, CLNO: String, CLNO_P: String, TIME: String, TYPE: String, DEMO: String, spark: SparkSession, properties: Properties) = {
    //    def insertSIEMCenter(in: trainResult, CLON: String, time: String, ty: Int) = {
    //val conn = new DBUtils().getPhoniexConnect("10.252.47.211,10.252.47.212,10.252.47.213,10.252.47.215:2181") //"10.252.47.211,10.252.47.212,10.252.47.213,10.252.47.215:2181"
    val conn = new DBUtils().getPhoniexConnect(properties.getProperty("zookeeper")) //"10.252.47.211,10.252.47.212,10.252.47.213,10.252.47.215:2181"
    conn.setAutoCommit(false)
    val state = conn.createStatement()
    //获取中心点的数据
    data.createOrReplaceTempView("OriData")
    val result = spark.sql("select distinct IP,FATHERPREDICTION,PREDICTION,RETURNCENTER from OriData")
    try {
      val tempResult = result.rdd.map { row =>
        val recordid: String = ROWUtils.genaralROW()
        //          val name:String = ""
        val index: String = row.getAs[Int]("PREDICTION").toString
        val info: String = row.getAs[List[String]]("RETURNCENTER").toArray.mkString("#")
        val index_p: String = row.getAs[String]("FATHERPREDICTION")
        val clno: String = CLNO.replace("-", "")
        val clno_p: String = CLNO_P.replace("-", "")
        val Type: String = TYPE
        val cltime: String = TIME
        val demo: String = DEMO
        //s"TCP#UDP#ICMP#GRE#tftime#packernum#bytesize"
        val dataori: String = "s"
        val name: String = row.getAs[String]("IP")
        List(recordid, index, index_p, info, clno, clno_p, Type, cltime, demo, dataori, name)
      }



      val lastdata: Array[List[String]] = tempResult.collect()
      logger.error("二级聚类插入hbase的示例数据:")
      //lastdata.take(10).foreach(println)
      for (row <- lastdata) {
        val sql = new StringBuilder()
          .append("upsert into T_SIEM_KMEANS_CENTER (\"ROW\",\"CENTER_INDEX\",\"CENTER_INDEX_P\",\"CENTER_INFO\",\"CLNO\",\"CLNO_P\",\"TYPE\",\"CLTIME\",\"DEMO\",\"DATA_ORIGIN\",\"NAME\") ")
          .append("values ('" + row(0) + "','" + row(1) + "','" + row(2) + "','" + row(3) + "','" + row(4) + "','" + row(5) + "'," + row(6) + ",'" + Timestamp.valueOf(row(7)) + "','" + row(8) + "','" + row(9) + "','" + row(10) + "')")
          .toString()
        state.execute(sql)
        //conn.commit()
      }
    } catch {
      case e: Exception => {
        logger.error(s"Insert into Hbase occur error!! " + e.getMessage)
        conn.rollback()
      }
    } finally {
      state.close()
      conn.close()
    }
  }

  //返回协议
  //传入列表、开始位置、协议长度
  def ReturnProto(FeaturesList: List[Double], startposition: Int, lengths: Int): List[String] = {

    var newlist: List[String] = List()
    //如果长度为4，则是一级聚类返回协议
    if (lengths == 4) {
      var values: Double = 0.0
      val protolist: List[String] = List("TCP", "UDP", "ICMP", "GRE")
      var proto: String = ""
      for (i <- startposition to lengths) {
        if (FeaturesList(i) > values) {
          values = FeaturesList(i)
          proto = protolist(i - startposition)
        }
      }

      for (j <- 0 to FeaturesList.length - 1) {
        if (j == 1) {}
        else if (j == 2) {}
        else if (j == 3) {}
        else if (j == 4) {}
        else if (j == 0) {
          newlist = newlist :+ proto
          newlist = newlist :+ FeaturesList(j).toInt.toString
        }
        else {
          newlist = newlist :+ FeaturesList(j).toInt.toString
        }
      }
    }
    //如果长度为6，则是二级聚类返回协议
    else if (lengths == 6) {
      var values: Double = 0.0
      val protolist: List[String] = List("HTTP", "QQ", "FTP", "TELNET", "DBAUDIT", "BDLOCALTROJAN")
      var proto: String = ""
      for (i <- startposition to lengths - 1) {
        if (FeaturesList(i) > values) {
          values = FeaturesList(i)
          proto = protolist(i - startposition)
        }
      }
      newlist = newlist :+ proto
    }
    newlist
  }
}
