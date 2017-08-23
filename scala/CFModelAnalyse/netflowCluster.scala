package CFModelAnalyse

import utils.DBUtils
import utils.ROWUtils
import java.io.InputStream
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties, UUID}
import com.github.nscala_time.time.Imports.{DateTime, DateTimeFormat}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.phoenix.schema.types.{PInteger, PTimestamp}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import scala.collection.mutable


/**
  * Created by huoguang on 2017/5/16.
  */
class netflowCluster {

  def clusterMain(properties: Properties,insertIntoHbase:Boolean = false) = {
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>聚类(data from netflow) start time:" + getNowTime() + ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    val spark: SparkSession = getSparkSession()
    val sc: SparkContext = spark.sparkContext
    val sql: SQLContext = spark.sqlContext //spark sql连接
    val netflowdata = netflowDataClean(sql,spark)//统计netflow建模数据
    val netflowModelData = createModelData(netflowdata,spark)//处理为 IP + FEATURES + SCLFEATURES 的dataframe
    val result = kMeansModel(spark, netflowModelData)
    val insertdata = ReturnCenterFirst(spark,result)
    //保存到HDFS
    import spark.implicits._
    val datag = insertdata.select("IP", "FEATURES", "SCLFEATURES", "LABEL", "CENTERPOINT","RETURNCENTER", "DISTANCE").rdd.map{row=>
      val name:String = row.getAs[String]("IP")
      val index: String = row.getAs[String]("LABEL")
      val distance:String = row.getAs[String]("DISTANCE").toString
      val info = row.getAs[Vector]("FEATURES").toArray//.mkString("#")
      (name,index,distance,info(0).toInt,info(1).toInt,info(2).toInt,info(3).toInt,info(4).toInt,info(5).toInt,info(6).toInt)
    }.toDF("IP", "LABEL", "DISTANCE","TCP","UDP","ICMP","GRE","TIME","PACKERNUM","BYTESIZE")
    val clusterResultNet = properties.getProperty("clusterResultPath.netflow")
    saveHDFS(datag,clusterResultNet,spark,sql)
    //此处插入病毒数据
    //    val spark: SparkSession = getSparkSession()
    //    val sc: SparkContext = spark.sparkContext
    //    val sql: SQLContext = spark.sqlContext //spark sql连接
    //    val netflowdata = netflowDataClean(sql,spark)//统计netflow建模数据
    //    val netflowdata1 = combineVirusData(netflowdata,spark,sql)
    //
    //    val netflowModelData = createModelData(netflowdata1,spark)//处理为 IP + FEATURES + SCLFEATURES 的dataframe
    //    val result = kMeansModel(spark, netflowModelData)
    //    val insertdata = ReturnCenterFirst(spark,result)
    //
    //    import spark.implicits._
    //    val datag = insertdata.select("IP", "FEATURES", "SCLFEATURES", "LABEL", "CENTERPOINT","RETURNCENTER", "DISTANCE").rdd.map{row=>
    //      val name:String = row.getAs[String]("IP")
    //      val index: String = row.getAs[String]("LABEL")
    //      val distance:String = row.getAs[String]("DISTANCE").toString
    //      val info = row.getAs[Vector]("FEATURES").toArray//.mkString("#")
    //      (name,index,distance,info(0),info(1),info(2),info(3),info(4),info(5),info(6))
    //    }.toDF("IP", "LABEL", "DISTANCE","TCP","UDP","ICMP","GRE","TIME","PACKERNUM","BYTESIZE")
    //    val saveOption = Map("header" -> "true", "delimiter" -> "\t", "path" -> "hdfs://172.16.12.38:9000/spark/modeldong/test/")
    //    datag.repartition(1).write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOption).save()//.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOption).save()//result文件夹不能创建
    //    println("data input success!")

    //入库
    //CENTER//CLUSTER
    print("聚类结果是否入库：")
    if(insertIntoHbase){
      println("Y")
      println("入库开始："+getNowTime())
      val CLNO = UUID.randomUUID().toString//生成本次计算的批次信息，UUID（流量）
      val TYPEFIRST = "1"//网络行为就为2
      val CLTIME = getNowTime()//生成本批次的时间
      val DEMOFIRST = s"TCP#UDP#ICMP#GRE#tftime#packernum#bytesize"// 第一次聚类指标描述
      insertSIEMCenter(insertdata,CLNO,CLTIME,TYPEFIRST,DEMOFIRST,spark)//插入中心数据
      insertSIEMCluster(insertdata,CLNO,CLTIME,DEMOFIRST,spark)//插入簇类数据
      println("入库结束："+getNowTime())
    }
    else{println("N")}
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>聚类(data from netflow) end time:" + getNowTime() + ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
  }

  def getNowTime(): String = {
    val format =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time = new Date().getTime
    format.format(time)
  }

  //连接
  def getSparkSession(): SparkSession = {
    //读取配置文件
    val properties: Properties = new Properties()
    val ipstream: InputStream = this.getClass().getResourceAsStream("/config.properties")
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


  /**
    * 数据处理
    */
  //读取netflow数据
  def getNetflowData(sql: SQLContext) = {
    val options = Map("header" -> "false", "delimiter" -> "\t", "path" -> "hdfs://172.16.12.38:9000/spark/netflowHDFS/2017-04-13")//hdfs://10.252.47.211:9000/spark/netflowHDFS/2017-04-13")////////////
    val dataall = sql.read.options(options).format("com.databricks.spark.csv").load()
    println("原始的数据：")
    dataall.show()
    dataall
  }

  def getNetflowDataFromHbase(sql:SQLContext,spark:SparkSession) = {
    val sc = spark.sparkContext
    println("状态：hbase连接开始")
    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址
    conf.set("hbase.zookeeper.quorum","hadoop110")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE,"T_SIEM_NETFLOW")//表名
    println("选择数据时间段: from "+getEndTime(-2)+ " until "+getNowTime())
    val start=Timestamp.valueOf(getEndTime(-2)).getTime
    val end=Timestamp.valueOf(getNowTime()).getTime
    val scan = new Scan()
    //    scan.setStartRow(Bytes.toBytes("1000021490287656"))
    //    scan.setStopRow(Bytes.toBytes("1004641489851462"))
    scan.setTimeRange(start,end)
    scan.setCaching(1000)
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    import spark.implicits._
    val hbaseRDD = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    val data = hbaseRDD.map(row => {
      val col1 = row._2.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("SRCIP"))
      val col2 = row._2.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("PROTO"))
      val col3 = row._2.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("PACKERNUM"))
      val col4 = row._2.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("BYTESIZE"))
      val col5 = row._2.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("STARTTIME"))
      val col6 = row._2.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("ENDTIME"))
      val col7 = row._2.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("RECORDTIME"))
      val flag=if(col1==null||col2==null||col3==null||col4==null||col5==null||col6==null||col7==null) 0 else 1
      (col1,col2,col3,col4,col5,col6,col7,flag)
    }).filter(_._8 > 0).map(line =>{
      (
        Bytes.toString(line._1),
        Bytes.toString(line._2),
        PInteger.INSTANCE.toObject(line._3).toString,
        PInteger.INSTANCE.toObject(line._4).toString,
        PTimestamp.INSTANCE.toObject(line._5).toString,
        PTimestamp.INSTANCE.toObject(line._6).toString,
        PTimestamp.INSTANCE.toObject(line._7).toString
      )
    }).toDF("SRCIP","PROTO","PACKERNUM","BYTESIZE","STARTTIME","ENDTIME","RECORDTIME")
    println("状态：读取hbase数据成功!!")
    data
  }

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def getEndTime(day:Int)={
    val now = getNowTime()
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, day)
    val time: Date = cal.getTime
    val newtime: String =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time)
    newtime
  }

  //netflow数据处理
  //0:内网攻击外网 1：外网攻击内网 2：内网攻击内网 3外网攻击外网
  def netflowDataClean(sql: SQLContext,spark:SparkSession) = {
    val starttime = getNowTime()
    println("状态：数据清理开始")
    //读取HDFS上的数据
    //val df = getNetflowData(sql)
    //读取hbase上的数据
    val df = getNetflowDataFromHbase(sql,spark)
    //筛选源ip和目的ip，并转化成DF
    df.createOrReplaceTempView("netFlowTemp")
    import spark.implicits._
    val df1 = sql.sql(
      s"""SELECT SRCIP,
         |case when PROTO ='TCP' then 1 else 0 end as TCP,
         |case when PROTO ='UDP' then 1 else 0 end as UDP,
         |case when PROTO = 'ICMP' then 1 else 0 end as ICMP,
         |case when PROTO = 'GRE' then 1 else 0 end as GRE,
         |PACKERNUM,BYTESIZE,STARTTIME,ENDTIME,RECORDTIME FROM netFlowTemp""".stripMargin).rdd.map{row=>
      val time = Timestamp.valueOf(row.getAs[String]("ENDTIME")).getTime - Timestamp.valueOf(row.getAs[String]("STARTTIME")).getTime
      (row.getAs[String]("SRCIP"),row.getAs[Int]("TCP"),row.getAs[Int]("UDP"),row.getAs[Int]("ICMP"),row.getAs[Int]("GRE"),time.toInt,row.getAs[String]("PACKERNUM").toInt,row.getAs[String]("BYTESIZE").toInt)
    }.toDF("IP","TCP","UDP","ICMP","GRE","COSTTIME","PACKERNUM","BYTESIZE")
    df1.createOrReplaceTempView("netFlowTemp1")
    //以IP分组统计
    val str =
      s"""SELECT IP,sum(TCP) TCP, sum(UDP) UDP, sum(ICMP) ICMP,sum(GRE) GRE,sum(COSTTIME) COSTTIME,sum(PACKERNUM) PACKERNUM,sum(BYTESIZE) BYTESIZE
         |FROM netFlowTemp1 GROUP BY IP""".stripMargin
    val sumData: DataFrame = spark.sql(str)
    sql.dropTempTable(s"netFlowTemp")
    sql.dropTempTable(s"netFlowTemp1")
    println("建模前数据（特征数据）：")
    sumData.show(5)
    val endtime = getNowTime()
    println("状态：数据清理结束")
    sumData
  }


  def createModelData(sumData: DataFrame, spark: SparkSession)= {
    //构建需要的DATAFRAME
    val starttime = getNowTime()
    import spark.implicits._
    //rdd to DF
    val dataST = sumData.rdd.map { line =>
      var temp1 = line.toSeq.toArray.map(_.toString)
      val temp2 = temp1.drop(1)
      (temp1(0), Vectors.dense(temp2.map(_.toDouble)))
    }.toDF("IP", "FEATURES")
    //      println("IP+FEATURES:")
    //      dataST.show(10)
    //归一化处理
    val scaler = new MinMaxScaler().setInputCol("FEATURES").setOutputCol("SCLFEATURES").fit(dataST)
    val sclData: DataFrame = scaler.transform(dataST)
    //    println("IP+FEATURES+SCLFEATURES")
    //    sclData.show(5)
    val endtime = getNowTime()
    println("状态：数据归一化完成")
    sclData
  }

  //读取病毒数据
  def combineVirusData(data:DataFrame,spark:SparkSession,sql: SQLContext) = {
    val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> "hdfs://172.16.12.38:9000/spark/modeldong/netflow.log")//hdfs://10.252.47.211:9000/spark/netflowHDFS/2017-04-13")////////////
    val virusdata = sql.read.options(options).format("com.databricks.spark.csv").load()
    virusdata.createOrReplaceTempView("virusdata")
    import spark.implicits._
    val virusdata1 = spark.sql(
      """select SRCIP,case when PROTO ='TCP' then 1 else 0 end as TCP,
        |case when PROTO ='UDP' then 1 else 0 end as UDP,
        |case when PROTO = 'ICMP' then 1 else 0 end as ICMP,
        |case when PROTO = 'GRE' then 1 else 0 end as GRE,
        STARTTIME,ENDTIME,PACKERNUM,BYTESIZE from virusdata""".stripMargin).rdd.map{row=>
      (row.getAs[String]("SRCIP"),row.getAs[Int]("TCP"),row.getAs[Int]("UDP"),row.getAs[Int]("ICMP"),row.getAs[Int]("GRE"),row.getAs[String]("ENDTIME").toInt-row.getAs[String]("STARTTIME").toInt,row.getAs[String]("PACKERNUM").toInt,row.getAs[String]("BYTESIZE").toInt)
    }.toDF("IP","TCP","UDP","ICMP","GRE","COSTTIME","PACKERNUM","BYTESIZE")
    virusdata1.createOrReplaceTempView("virusdata1")
    val virusdata2 = spark.sql("select IP,SUM(TCP) TCP,SUM(UDP) UDP,SUM(ICMP) ICMP,SUM(GRE) GRE,SUM(COSTTIME) COSTTIME,SUM(PACKERNUM) PACKERNUM,SUM(BYTESIZE) BYTESIZE from virusdata1 group by IP")
    virusdata2.show(100,false)

    //整合到数据
    data.printSchema()
    val newdata = data.union(virusdata2)
    newdata
  }


  /**
    *聚类过程
    */
  //聚类主函数
  def kMeansModel(spark: SparkSession, dataDF: DataFrame) = {
    val sc: SparkContext = spark.sparkContext
    val sql: SQLContext = spark.sqlContext
    val starttime = getNowTime()
    println("状态：kmeans开始 " + starttime)
    //聚类
    //设置k值
    var k =10 //传入轮廓系数前的k
    val numIterations: Int = 20//迭代次数
    //判断k的取值是否适合做聚类
    val nData: Int = dataDF.count().toInt
    if (nData <= k & nData>2) {k = nData - 1}
    if (nData <= 2) {k = 1}
    if (nData > k) {k = k}
    val kMeansModel: KMeansModel = new KMeans().setK(k).setFeaturesCol("SCLFEATURES").setPredictionCol("LABEL").fit(dataDF)//.setMaxIter(numIterations)
    val result: DataFrame = kMeansModel.transform(dataDF)//DF
    //
    println("处理聚类离群点："+ getNowTime())
    var target:List[Int] = Nil
    (0 to k-1).toList.foreach { eachk =>
      if(result.filter(s"LABEL='$eachk'").count()<2){
        target = target :+ eachk
        println("簇："+eachk+" 含有的记录数："+result.filter(s"LABEL='$eachk'").count())
      }
    }
    println("离群点："+target)
    //簇中心和簇点
    val clusterCenters = kMeansModel.clusterCenters
    val centersList = clusterCenters.toList
    val centersListBD = spark.sparkContext.broadcast(centersList)
    import spark.implicits._
    val kmeansTrainOne: DataFrame = result.rdd.map {
      row =>
        val centersListBDs: List[Vector] = centersListBD.value
        val IP = row.getAs[String]("IP")
        val FEATURES = row.getAs[Vector]("FEATURES")
        val SCLFEATURES = row.getAs[Vector]("SCLFEATURES")
        var LABEL:String = row.getAs[Int]("LABEL").toString
        if(target.contains(row.getAs[Int]("LABEL"))){LABEL = "other"}//给离群点打标签
      val CENTERPOINT: Vector = centersListBDs(row.getAs[Int]("LABEL"))//通过label获取中心点坐标
      val DISTANCE = math.sqrt(CENTERPOINT.toArray.zip(SCLFEATURES.toArray).map(p => p._1 - p._2).map(d => d * d).sum).toString//距离
        (IP,FEATURES,SCLFEATURES,LABEL,CENTERPOINT,DISTANCE)
    }.toDF("IP", "FEATURES", "SCLFEATURES", "LABEL", "CENTERPOINT", "DISTANCE")
    println("状态：kmeans结束 "+ getNowTime())
    kmeansTrainOne
  }

  //返回中心点
  def ReturnCenterFirst(spark: SparkSession, kMeansOneDF: DataFrame) ={
    //定义一个空的map
    var CenterMap = mutable.Map.empty[Int, List[Int]]
    //创建表名
    val sql: SQLContext = spark.sqlContext
    kMeansOneDF.createOrReplaceTempView("BIGDATA")
    val predictionnum: Array[Int] = sql.sql(s"SELECT DISTINCT LABEL FROM BIGDATA WHERE LABEL<>'other'").select("LABEL").rdd.map(r => r(0).toString.toDouble.toInt).collect()
    for (label <- predictionnum) {
      //簇点长度
      var FeaturesLen: Int = sql.sql("SELECT COUNT(*) FROM BIGDATA WHERE LABEL='" + label.toString + "'").collect().head.getLong(0).toInt
      //计算中心坐标//归一化前的特征
      var FeaturesList= sql.sql("SELECT FEATURES FROM BIGDATA WHERE LABEL='" + label.toString + "'").select("FEATURES").rdd
        .map {
          line =>
            var tmp: List[Double] = Nil
            line(0) match {
              case vec: org.apache.spark.ml.linalg.Vector => tmp = vec.toArray.toList
              case _ => 0
            }
            tmp
        }.reduce {
        (x, y) => x.zip(y).map(p => (p._1 + p._2))
      }.map(p => p / FeaturesLen)
      //构造成一个map
      CenterMap += (label -> FeaturesList.map(_.toInt))//label->List(中心点反归一化的值)
    }
    //    println(CenterMap)
    import spark.implicits._
    val ResultDF= kMeansOneDF.rdd.map {
      row =>
        val IP = row.getAs[String]("IP")
        val FEATURES = row.getAs[Vector]("FEATURES")
        val SCLFEATURES = row.getAs[Vector]("SCLFEATURES")
        val LABEL = row.getAs[String]("LABEL")
        val CENTERPOINT = row.getAs[Vector]("CENTERPOINT")
        val DISTANCE = row.getAs[String]("DISTANCE")
        //反归一化的中心点
        var RETURNCENTER :List[Int]= Nil
        if (LABEL=="other"){
          RETURNCENTER = RETURNCENTER :+ -1
          //          RETURNCENTER = RETURNCENTER ++ SCLFEATURES.toArray.toList.map(_.toInt)
        }else {
          RETURNCENTER = CenterMap(LABEL.toDouble.toInt)
        }
        (IP, FEATURES, SCLFEATURES, LABEL, CENTERPOINT, RETURNCENTER, DISTANCE)
    }.toDF("IP", "FEATURES", "SCLFEATURES", "LABEL", "CENTERPOINT","RETURNCENTER", "DISTANCE")
    //.toDF("IP", "FEATURES", "SCLFEATURES", "LABEL", "CENTERPOINT","RETURNCENTER", "DISTANCE")
    println("结果数据:")
    ResultDF.show(5)
    ResultDF
  }

  /**
    *插入数据到hbase
    */
  //插入中心点数据到hbase
  def insertSIEMCenter(data:DataFrame,CLNO:String,TIME:String,TYPE:String,DEMO:String,spark: SparkSession) = {
    val conn = new DBUtils().getPhoniexConnect("10.252.47.211,10.252.47.212,10.252.47.213,10.252.47.215:2181")//("hadoop:2181")////
    conn.setAutoCommit(false)
    val state = conn.createStatement()
    //获取中心点的数据
    data.createOrReplaceTempView("OriData")
    val result = spark.sql("select distinct LABEL,RETURNCENTER from OriData").toDF()
    try {
      val tempResult = result.rdd.map { row =>
        val recordid: String = ROWUtils.genaralROW()
        val index: String = row.getAs[String]("LABEL")
        val info: String = row.getAs[List[Int]]("RETURNCENTER").mkString("#")
        val clno: String = CLNO.replace("-","")
        val Type: String = TYPE
        val cltime: String = TIME
        val demo: String = DEMO
        //s"TCP#UDP#ICMP#GRE#tftime#packernum#bytesize"
        val dataori: String = "g"
        List(recordid, index, info, clno, Type, cltime, demo, dataori)
      }
      val lastdata: Array[List[String]] = tempResult.collect()
      println("一级聚类，插入hbase的示例数据:")
      lastdata.take(3).foreach(println)
      for (row <- lastdata) {
        val sql = new StringBuilder()
          .append("upsert into T_SIEM_KMEANS_CENTER (\"ROW\",\"CENTER_INDEX\",\"CENTER_INFO\",\"CLNO\",\"TYPE\",\"CLTIME\",\"DEMO\",\"DATA_ORIGIN\") ")
          .append("values ('" + row(0) + "','" + row(1) + "','" + row(2) + "','" + row(3) + "'," + row(4) + ",'" + Timestamp.valueOf(row(5)) + "','" + row(6) + "','" + row(7) + "')")
          .toString()
        //          .append("values ('" + recordid + "','" + index + "','" + info + "','" + clno + "'," + Type + ",'"  + Timestamp.valueOf(cltime) +"','" + demo+"','" + dataori + "')" )
        state.execute(sql)
        conn.commit()
      }
      println(s"insert into hbase success!")
    } catch {
      case e: Exception => {
        println(s"Insert into Hbase occur error!! " + e.getMessage)
        conn.rollback()
      }
    } finally {
      state.close()
      conn.close()
    }
  }

  //聚类簇点插入到hbase
  def insertSIEMCluster(data:DataFrame,CLNO:String,TIME:String,DEMO:String,spark: SparkSession) = {
    val conn = new DBUtils().getPhoniexConnect("10.252.47.211,10.252.47.212,10.252.47.213,10.252.47.215:2181")//("hadoop:2181")//(//
    conn.setAutoCommit(false)
    val state = conn.createStatement()
    //获取中心点的数据
    try{
      val result = data.rdd.map { row =>
        val recordid: String = ROWUtils.genaralROW()
        val name:String = row.getAs[String]("IP")
        val index: String = row.getAs[String]("LABEL")
        val clno: String = CLNO.replace("-","")
        val distance:String = row.getAs[String]("DISTANCE").toString
        val info: String = row.getAs[Vector]("FEATURES").toArray.mkString("#")
        val cltime:String = TIME
        val demo:String = DEMO//s"TCP#UDP#ICMP#GRE#tftime#packernum#bytesize"
      val dataori:String = "g"
        List(recordid, clno, name, index, distance, info, cltime, demo, dataori)
      }
      val lastdata: Array[List[String]] = result.collect()
      println("聚类后簇点，插入hbase的示例数据:")
      lastdata.take(3).foreach(println)
      for (row <- lastdata) {
        val sql = new StringBuilder()
          .append("upsert into T_SIEM_KMEANS_CLUSTER (\"ROW\",\"CLNO\",\"NAME\",\"CENTER_INDEX\",\"DISTANCE\",\"CLUSTER_INFO\",\"CLTIME\",\"DEMO\",\"DATA_ORIGIN\") ")
          .append("values ('" + row(0) + "','" + row(1) + "','" + row(2) + "','" + row(3) + "'," + row(4) + ",'" + row(5) + "','" + Timestamp.valueOf( row(6)) +"','" +  row(7)+"','" +  row(8) + "')" )
          .toString()
        state.execute(sql)
        conn.commit()
      }
      println(s"insert into hbase success!")
    } catch {
      case e: Exception => {
        println(s"Insert into Hbase occur error!! " + e.getMessage)
        conn.rollback()
      }
    } finally {
      state.close()
      conn.close()
    }
  }

  def saveHDFS(data:DataFrame,path:String,spark:SparkSession,sql: SQLContext) {
    val saveOption = Map("header" -> "true", "delimiter" -> "\t", "path" -> path)
    data.repartition(1).write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOption).save()//.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOption).save()//result文件夹不能创建
    println("结果保存在："+path)

  }


}
