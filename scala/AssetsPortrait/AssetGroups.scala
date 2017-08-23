package AssetsPortrait

import java.io.{BufferedInputStream, File, FileInputStream, InputStream}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties, UUID}

import utils.HDFSUtils
import org.apache.log4j.{Level, Logger, PropertyConfigurator}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{MinMaxScaler, StandardScaler, StandardScalerModel}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext



/**
  * Created by Dong on 2017/6/28.
  */

object AssetGroups extends HbaseProcessGroup with HDFSUtils{

  //  日志
  @transient lazy val logger = Logger.getLogger(this.getClass)
//  val directory = new File("..")
//  val filePath = directory.getAbsolutePath //设定为上级文件夹 获取绝对路径
  val logFilePath = System.getProperty("user.dir")
  PropertyConfigurator.configure(logFilePath + "/conf/clusterlog4j.properties")
  Logger.getLogger("org").setLevel(Level.WARN) //显示的日志级别

  def main(args: Array[String]) {
    //读取配置文件
    val filePath = System.getProperty("user.dir")
    val properties: Properties = new Properties()
    val ipstream = new BufferedInputStream(new FileInputStream(filePath + "/conf/cluster.properties"))
    properties.load(ipstream)

    val time1 = getNowTime()
    val spark: SparkSession = getSparkSession()
    val sc: SparkContext = spark.sparkContext
    val sql: SQLContext = spark.sqlContext //spark sql连接

    //阈值分组
    logger.error(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>分组开始："+getNowTime()+">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    //读取数据
    //read data from HDFS
    val getnetflowdata = getNetflowData(sql,properties)
    //the process of data
    val netflowdata = netflowDataClean(getnetflowdata,spark,sql,properties)
    //scala
    val scldata = sclprocess(netflowdata, spark)
    //add weight to each sample and create two feature
    val weightdata = bindByWeight(scldata,properties,spark)
    //add label to each sample
    val labeldata = addLabelUseMean(weightdata,spark,sql)
    //calculate the center's feature of each label
    val centerdata = calculateCenter(labeldata,spark,sql)
    //compare with the labels that was calculated last time
    val comparedata = compareLastResult(centerdata,spark,sql,properties)
    logger.error(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>分组结束："+getNowTime()+">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

    //入库(CENTER/CLUSTER)
    logger.error(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>数据储存开始："+getNowTime()+">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    val CLNO = UUID.randomUUID().toString//生成本次计算的批次信息，UUID（流量）
    val TYPEFIRST = "1"//网络行为就为2
    val CLTIME = getNowTime()//生成本批次的时间
    val DEMOFIRST = s"TCP#UDP#ICMP#GRE#tftime#packernum#bytesize"// 第一次聚类指标描述
    //保存到HBASE
    //    insertSIEMCenter(insertdata,CLNO,CLTIME,TYPEFIRST,DEMOFIRST,spark)
    //    insertSIEMCluster(insertdata,CLNO,CLTIME,DEMOFIRST,spark)
    //保存到HDFS
    saveCenterCsvNew(comparedata,CLNO,CLTIME,TYPEFIRST,DEMOFIRST,spark,properties)//插入中心数据
    saveClusterCsvNew(comparedata,CLNO,CLTIME,DEMOFIRST,spark,properties)//插入簇类数据
    //    saveToLocal()
    HDFSToLocal(properties)
    logger.error(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>数据储存结束："+getNowTime()+">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    spark.stop()
  }


  def getSparkSession(): SparkSession = {
    //读取配置文件
    val filePath = System.getProperty("user.dir")
    val properties: Properties = new Properties()
    val ipstream = new BufferedInputStream(new FileInputStream(filePath + "/conf/cluster.properties"))
    //val ipstream: InputStream = this.getClass().getResourceAsStream("/config.properties")
    properties.load(ipstream)

    val masterUrl = properties.getProperty("spark.master.url")
    val appName = properties.getProperty("spark.app.name")
    val sparkconf = new SparkConf()
      .setMaster(masterUrl)
      .setAppName(appName)
      .set("spark.port.maxRetries", "100")
    val Spark = SparkSession.builder().config(sparkconf).getOrCreate()
    Spark
  }

//  def getNetflowData(sql: SQLContext,properties: Properties) = {
//    val hdfs = properties.getProperty("hdfs.base.path")
//    val netflow = properties.getProperty("netflow.hdfs.origin")
//    //    val path = "hdfs://bluedon155:9000/spark/data/netflow/" + getEndTime(-1)
//    //    val path = "hdfs://172.16.12.38:9000/spark/data/netflowHDFS/" + getEndTime(-1)
//    val today = new SimpleDateFormat("yyyy-MM-dd").format(new Date().getTime)
//    val path = hdfs + netflow + getEndTime(-1)+"--"+today
//    val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> path)
//    val dataall = sql.read.options(options).format("com.databricks.spark.csv").load()
//    println("原始数据的日期："+getEndTime(-1)+",原始数据示例：")
//    dataall.show(5)
//    dataall
//  }

  def getNetflowData(sql: SQLContext,properties: Properties) = {
    //hdfs数据路径
    val hdfs = properties.getProperty("hdfs.base.path")
    val netflow = properties.getProperty("netflow.hdfs.origin")
    //遍历读取最近7天数据
    val listdata: List[DataFrame] = (-7 to -1).map {
      each =>
        val path = hdfs + netflow + getDayTime(each) + "--" + getDayTime(each + 1)
        var datasave: DataFrame = null
        try {
          val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> path)
          datasave = sql.read.options(options).format("com.databricks.spark.csv").load()
          logger.error("读取成功："+ path)
        } catch {
          case e: Exception => logger.error("读取出错："+ path)
        } finally {}
        datasave
    }.toList.filter(_ != null)
    //合并最近7天的数据
    var uniondata:DataFrame = null
    if (listdata.length > 1) {
      val unionFun = (a: DataFrame, b: DataFrame) => a.union(b).toDF
      uniondata = listdata.tail.foldRight(listdata.head)(unionFun)
    }
    else {
      uniondata = listdata(0)
    }
    uniondata.show(5, false)
    uniondata
  }

  //0:内网攻击外网 1：外网攻击内网 2：内网攻击内网 3外网攻击外网
  def netflowDataClean(data:DataFrame,spark: SparkSession,sql: SQLContext,properties: Properties) = {
    logger.error("数据清理开始："+ getNowTime() + ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    //筛选源ip和目的ip，并转化成DF
    data.createOrReplaceTempView("netFlowTemp")
    val df1 = spark.sql(s"""SELECT FLOWD,SRCIP IP,COSTTIME,TCP,UDP,ICMP,GRE,PACKERNUM,BYTESIZE FROM netFlowTemp WHERE FLOWD='0' OR FLOWD='2'""".stripMargin)
    val df2 = spark.sql(s"""SELECT FLOWD,DSTIP IP,COSTTIME,TCP,UDP,ICMP,GRE,PACKERNUM,BYTESIZE FROM netFlowTemp WHERE FLOWD='1'""".stripMargin)
    val df3 = df1.union(df2)
    sql.dropTempTable(s"netFlowTemp")
    df3.createOrReplaceTempView("netFlowTemp1")
    //以IP分组统计
    val str =
      s"""SELECT IP,sum(TCP) TCP, sum(UDP) UDP, sum(ICMP) ICMP,sum(GRE) GRE,sum(COSTTIME) COSTTIME,sum(PACKERNUM) PACKERNUM,sum(BYTESIZE) BYTESIZE
         |FROM netFlowTemp1 GROUP BY IP limit 200""".stripMargin
    val sumData: DataFrame = spark.sql(str)
    sql.dropTempTable(s"netFlowTemp1")
    logger.error("数据清理结果：")
    sumData.show(5,false)
    //此处插入资产设备内的过滤条件
    val resultdata = filterTargetIP(sumData,spark)
    val endtime = getNowTime()
    logger.error("数据清理结束："+endtime + ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    resultdata.show(10,false)
    resultdata
  }

  def filterTargetIP(data:DataFrame,spark: SparkSession) = {
    //读取文件中的IP
    val sc: SparkContext = spark.sparkContext
    val ip = sc.textFile("./conf/targetIP.csv").collect()
    val resultdata = data.filter{line =>
      ip.contains(line.getAs[String]("IP"))==true
    }
    logger.error("读取的IP段数据：")
    resultdata.show(5,false)
    resultdata
  }

  def sclprocess(sumData: DataFrame, spark: SparkSession)= {
    import spark.implicits._
    //生成建模需要的格式数据
    val dataST = sumData.rdd.map { line =>
      var temp1 = line.toSeq.toArray.map(_.toString)
      val temp2 = temp1.drop(1)//去掉IP列
      (temp1(0), Vectors.dense(temp2.map(_.toDouble)))
    }.toDF("IP", "FEATURES")
    //归一化处理
    val scaler = new MinMaxScaler().setInputCol("FEATURES").setOutputCol("SCLFEATURES").fit(dataST)
    val sclData: DataFrame = scaler.transform(dataST)
//    val resultdata = sclData.rdd.map{
//      row =>
//        val ip = row.getAs[String]("IP")
//        val feature = row.getAs[Vector]("FEATURES")
//        var sclfeatures = row.getAs[Vector]("SCLFEATURES")
//        (ip,feature,sclfeatures)
//    }.toDF("IP","FEATURES","SCLFEATURES")
//    println(resultdata.schema)
//    resultdata.show(10,false)
    sclData
  }

  //加权合并
  def bindByWeight(data:DataFrame,properties: Properties,spark:SparkSession)= {
    logger.error("添加权重开始："+getNowTime() + ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    //读取配置文件
    val w1 = List("weight.proto.tcp","weight.proto.udp","weight.proto.icmp","weight.proto.gre").map(properties.getProperty(_).toDouble)
    val w2 = List("weight.flow.trantime","weight.flow.packernum","weight.flow.bytesize").map(properties.getProperty(_).toDouble)
    //将7个特征合并成2个特征（协议、流量）
    import spark.implicits._
    val dataweight = data.rdd.map {
      row =>
        val ip = row.getAs[String]("IP")
        val feature = row.getAs[Vector]("FEATURES")
        val sclfeatures = row.getAs[Vector]("SCLFEATURES").toArray
        val proto = w1(0) * sclfeatures(0) + w1(1) * sclfeatures(1) + w1(2) * sclfeatures(2) + w1(3) * sclfeatures(3)
        val flow = w2(0) * sclfeatures(4) +w2(1) * sclfeatures(5) + w2(2) *sclfeatures(6)
        (ip, feature,row.getAs[Vector]("SCLFEATURES"), proto, flow)
    }.toDF("IP","FEATURES","SCLFEATURES", "PROTO", "FLOW")
    logger.error("添加权重结束："+getNowTime() + ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    dataweight
  }

  //添加标签(0-低协议低流量，1-低协议高流量，2-高协议低流量，3-高协议高流量)
  def addLabelUseMean(data:DataFrame,spark:SparkSession,sql: SQLContext)= {
    logger.error("添加标签开始："+getNowTime() + ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    //计算两个特征的均值-2*标准差和均值+2*标准差
    val colmean: List[Double] = List("PROTO","FLOW").map(data.select(_).rdd.map(_(0).toString.toDouble).mean())
    val colstdev: List[Double] = List("PROTO","FLOW").map(data.select(_).rdd.map(_(0).toString.toDouble).stdev())
    val colbound: List[Double] = colmean.zip(colstdev).map(row => row._1+3*row._2)
    logger.error("E(proto)=" + colmean(0) + "\tE(flow)=" + colmean(1))
    logger.error("std(proto)=" + colstdev(0) + "\tstd(flow)=" + colstdev(1))
    logger.error("E-2*std=" + colbound(0) + "\tflow:E+2*std=" + colbound(1))

    //筛选出不满足的数据(识为离群点)
    val protobound = colbound(0)
    val flowbound = colbound(1)
    import spark.implicits._
    val outboundprotodata = data.filter(s"PROTO>$protobound").rdd.map{row =>
      (row.getAs[String]("IP"),row.getAs[Vector]("FEATURES"),row.getAs[Vector]("SCLFEATURES"),row.getAs[Double]("PROTO"),row.getAs[Double]("FLOW"),"other_high_proto")
    }.toDF("IP","FEATURES","SCLFEATURES", "PROTO", "FLOW","LABEL")
    val outboundflow = data.filter(s"FLOW>$flowbound").rdd.map{row =>
      (row.getAs[String]("IP"),row.getAs[Vector]("FEATURES"),row.getAs[Vector]("SCLFEATURES"),row.getAs[Double]("PROTO"),row.getAs[Double]("FLOW"),"other_high_flow")
    }.toDF("IP","FEATURES","SCLFEATURES", "PROTO", "FLOW","LABEL")
    val combindoutlier = outboundprotodata.union(outboundflow)
//    logger.error("离群点数据：")
//    combindoutlier.show(5,false)
    //筛选出正常IP的数据
    val otherip = outboundprotodata.select("IP").union(outboundflow.select("IP")).map(_(0).toString).rdd
    val normalip = data.select("IP").rdd.map(_(0).toString).subtract(otherip).toDF("IP")
    normalip.createOrReplaceTempView("boundip")
    data.createOrReplaceTempView("addlabeldata")
    val filterdata = spark.sql("select IP,FEATURES,SCLFEATURES,PROTO,FLOW from addlabeldata where IP in (select IP from boundip)")
    //对正常IP进行标识
    import spark.implicits._
    val labeldata = filterdata.rdd.map{
      row =>
        val ip = row.getAs[String]("IP")
        val feature = row.getAs[Vector]("FEATURES")
        val sclfeature = row.getAs[Vector]("SCLFEATURES")
        val proto = row.getAs[Double]("PROTO")
        val flow = row.getAs[Double]("FLOW")
        var label = ""
        if(proto<=colmean(0) && flow<=colmean(1)){label = "low_low"}
        if(proto<=colmean(0) && flow>colmean(1)){label = "low_high"}
        if(proto>colmean(0) && flow<=colmean(1)){label = "high_low"}
        if(proto>colmean(0) && flow>colmean(1)){label = "high_high"}
        (ip,feature,sclfeature,proto,flow,label)
    }.toDF("IP","FEATURES","SCLFEATURES","PROTO","FLOW","LABEL")
    val result = labeldata.union(combindoutlier)
    logger.error("所有标签数据：")
    result.show(5,false)
    logger.error("添加标签结束：" + getNowTime() + ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    result
  }

  //计算中心点
  def calculateCenter(data:DataFrame,spark:SparkSession,sql: SQLContext)={
    logger.error("计算中心点开始：" + getNowTime() + ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    import spark.implicits._
    //计算与中心距离
    val minmax = data.select("FEATURES").rdd.map(line=>line.getAs[Vector]("FEATURES").toArray)
    val tcp = minmax.map(line=>line(0)).max()
    val udp = minmax.map(line=>line(1)).max()
    val icmp = minmax.map(line=>line(2)).max()
    val gre = minmax.map(line=>line(3)).max()
    val trantime = minmax.map(line=>line(4)).max()
    val packernum = minmax.map(line=>line(5)).max()
    val bytesize = minmax.map(line=>line(6)).max()
    val maxcenterlist = Array(tcp,udp,icmp,gre,trantime,packernum,bytesize)

    // /正常类
    val target = List("low_low","low_high","high_low","high_high")
    val label = data.select("LABEL").filter{
      each =>
        target.contains(each.getAs[String]("LABEL"))
    }.distinct().map(_(0).toString).collect().toList
    val labeldata: List[DataFrame] = label.map{
      line=>
        //将FEATURES字段分开
        val tempdata = data.filter {row=>
        row.getAs[String]("LABEL")==line
      }.rdd.map{row=>
        val ip = row.getAs[String]("IP")
        val feature = row.getAs[Vector]("FEATURES")
        val label = row.getAs[String]("LABEL")
        (ip,feature,row.getAs[Vector]("SCLFEATURES"),label,feature.toArray(0),feature.toArray(1),feature.toArray(2),feature.toArray(3),feature.toArray(4),feature.toArray(5),feature.toArray(6))
      }.toDF("IP","FEATURES","SCLFEATURES","LABEL","TCP","UDP","ICMP","GRE","TRANTIME","PACKERNUM","BYTESIZE")
      //计算CENTER
      val allfeature = List("TCP","UDP","ICMP","GRE","TRANTIME","PACKERNUM","BYTESIZE")
      val allfeaturelist = allfeature.map(tempdata.select(_).map(_(0).toString.toDouble).rdd.mean())//.mkString("#")
      val sclcenter = allfeaturelist.zip(maxcenterlist).map(row=>minmaxscala(row._1,row._2)).toArray

      //加上CENTER
      tempdata.map{line=>
        val ip = line.getAs[String]("IP")
        val feature = line.getAs[Vector]("FEATURES").toArray.map(_.toInt).mkString("#")
        val label = line.getAs[String]("LABEL")
        val returnfeature = allfeaturelist.map(_.toInt).mkString("#")
        val sclfeature = line.getAs[Vector]("SCLFEATURES").toArray
        //        println("insert.sclfeature:"+sclfeature.toList)
        val a = sclfeature.zip(sclcenter)//.map(p => p._1 - p._2).map(d => d * d).sum
      val distance = math.sqrt(sclfeature.zip(sclcenter).map(p => p._1 - p._2).map(d => d * d).sum).toString//距离
        //        (ip,feature,label,feature(0),feature(1),feature(2),feature(3),feature(4),feature(5),feature(6),allfeaturelist.mkString("#"),distance)
        //      }.toDF("IP","FEATURES","LABEL","TCP","UDP","ICMP","GRE","TRANTIME","PACKERNUM","BYTESIZE","RETURNCENTER","DISTANCE")
        (ip,feature,label,returnfeature,distance)
      }.toDF("IP","FEATURES","LABEL","RETURNCENTER","DISTANCE")
    }

    //其他类
    val othertarget = List("other_high_proto","other_high_flow")
    val otherlabel = data.select("LABEL").filter{
      each =>
        othertarget.contains(each.getAs[String]("LABEL"))
    }.distinct().map(_(0).toString).collect().toList

    val otherlabeldata = otherlabel.map{
      each=>
        data.filter{
          row=>
            row.getAs[String]("LABEL")==each
      }.rdd.map{line=>
        val ip = line.getAs[String]("IP")
        val feature = line.getAs[Vector]("FEATURES").toArray.map(_.toInt).mkString("#")
        val label = line.getAs[String]("LABEL")
        val center = "0"
        val distance = "0"
        (ip,feature,label,center,distance)
      }.toDF("IP","FEATURES","LABEL","RETURNCENTER","DISTANCE")
    }

    //union数据(离群类和正常类)
    val unionFun = (a:DataFrame, b:DataFrame) =>a.union(b).toDF
    var unionlabeldata:DataFrame = null
    if(labeldata.length != 1){
      unionlabeldata = labeldata.tail.foldRight(labeldata.head)(unionFun)
    }
    else{
      unionlabeldata = labeldata.head
    }

    var unionotherlabeldata:DataFrame = null
    if(labeldata.length != 1){
      unionotherlabeldata = otherlabeldata.tail.foldRight(otherlabeldata.head)(unionFun)
    }
    else{
      unionlabeldata = otherlabeldata.head
    }

    println("正常类:")
    unionlabeldata.show(5,false)
    println("other类:")
    unionotherlabeldata.show(5,false)
    val uniondata = unionlabeldata.union(unionotherlabeldata)
    logger.error("计算中心点结束：" + getNowTime() + ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

    uniondata
  }


  def minmaxscala(x:Double,max:Double,min:Double=0.0)={
    var result = 0.0
    if(max!=0.0){result = (x-min)/(max-min)}
    result
  }


  def compareLastResult(data:DataFrame,spark: SparkSession,sql: SQLContext,properties: Properties)={
    //读取上次模型结果
    logger.error("关联上次结果开始："+getNowTime() + ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    import spark.implicits._
    val hdfs = properties.getProperty("hdfs.base.path")
    val netflow = properties.getProperty("cluster.hdfs.path2")
    val path = hdfs + netflow
    val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> path)
    var flag = false
    var lastresult:DataFrame = null
    var result:DataFrame = null
    try{
      lastresult = sql.read.options(options).format("com.databricks.spark.csv").load()
      flag = true
    } catch {
      case e: Exception => println("出错：" + e.getMessage)
    }finally {}
    //判断是否读取上次结果成功
    if(flag){
      logger.error("上次模型结果数据示例：")
      lastresult.show(5)
      lastresult.createOrReplaceTempView("lastresult")
      data.createOrReplaceTempView("modelresult")
      val str = "select distinct t.*,s.CENTER_INDEX LAST_LABEL from modelresult t left join lastresult s on t.IP = s.NAME"
      val comparelastreuslt = spark.sql(str)
      comparelastreuslt.createOrReplaceTempView("comparelastreuslt")
      val str2 =
        s"""select IP,FEATURES,LABEL,RETURNCENTER,DISTANCE,LAST_LABEL,CASE WHEN LABEL=LAST_LABEL THEN "1" ELSE "0" END AS COMPARE
           |from comparelastreuslt""".stripMargin
      result = spark.sql(str2)
      result.show(5,false)
//      result = comparelastreuslt.rdd.map{
//        line=>
//          val ip = line.getAs[String]("IP")
//          val feature = line.getAs[String]("FEATURES")
//          val label = line.getAs[String]("LABEL")
//          val returncenter = line.getAs[String]("RETURNCENTER")
//          val distance = line.getAs[String]("DISTANCE")
//          val last_label = line.getAs[String]("LAST_LABEL")
//          var compare = ""
//          if(label == last_label){
//            compare = "0"
//          }
//          else{
//            compare = "1"
//          }
//          //      (ip,feature,label,returncenter,distance,last_label,compare)
//          //    }.toDF("IP","FEATURES","LABEL","RETURNCENTER","DISTANCE","COMPARELASTRESULT")
//          (ip,feature,label,returncenter,distance,last_label,compare)
//      }.toDF("IP","FEATURES","LABEL","RETURNCENTER","DISTANCE","LAST_LABEL","COMPARE")
    }else{
      result = data.map{line=>
        val ip = line.getAs[String]("IP")
        val feature = line.getAs[String]("FEATURES")
        val label = line.getAs[String]("LABEL")
        val returncenter = line.getAs[String]("RETURNCENTER")
        val distance = line.getAs[String]("DISTANCE")
        val last_label = "0"
        val compare = "0"
        (ip,feature,label,returncenter,distance,last_label,compare)
      }.toDF("IP","FEATURES","LABEL","RETURNCENTER","DISTANCE","LAST_LABEL","COMPARE")
    }
    logger.error("关联结果：")
    result.show(10,false)
    logger.error("关联上次结果结束："+getNowTime() + ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    result
  }


  def getDayTime(day:Int)={
    val now = getNowTime()
    //    println(now)
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, day)
    val time: Date = cal.getTime
    val newtime: String =new SimpleDateFormat("yyyy-MM-dd").format(time)
    newtime
  }



}
