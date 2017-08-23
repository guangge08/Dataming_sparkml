//package CFModelAnalyse
//
//
//
//
//import utils.HbaseAPI
//import java.io.{BufferedInputStream, FileInputStream, InputStream}
//import java.sql.Timestamp
//import java.text.SimpleDateFormat
//import java.util.{Calendar, Date, Properties}
//
//import org.apache.hadoop.hbase.HBaseConfiguration
//import org.apache.hadoop.hbase.client.{Result, Scan}
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat
//import org.apache.hadoop.hbase.protobuf.ProtobufUtil
//import org.apache.hadoop.hbase.util.{Base64, Bytes}
//import org.apache.phoenix.schema.types.{PInteger, PTimestamp}
//import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
//import org.apache.spark.ml.feature.MinMaxScaler
//import org.apache.spark.ml.linalg.{Vector, Vectors}
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql._
//import org.apache.spark.rdd._
//import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
//import org.apache.spark.mllib.evaluation.MulticlassMetrics
//
///**
//  * Created by Dong on 2017/6/20.
//  */
//class netflowClusterSec extends HbaseAPI{
//
//  def clustermain(properties: Properties,spark: SparkSession, sql: SQLContext)={
//    //获取hbase上的数据
//    val getnetflowdata = getNetflowDataFromHbase(sql,spark,properties,false)
////    val getnetflowdata = getNetflowDataFromHbaseSec(properties,spark)
////    val getnetflowdata = getNetflowDataFromHbaseLocal(sql)
//    //数据清理
//    val cleandata = netflowDataClean(getnetflowdata,sql,spark)
//    //添加权重
//    val weightdata = bindByWeight(cleandata,properties,spark)
//    //数据标签
//    val (labeldata,bound) = addLabelUseMean(weightdata,spark,sql)
//    //归一化
//    val scaledata = scaleData(labeldata,spark)
//    //聚类
//    val clusterresult = kMeansModel(scaledata , spark)
//    //计算类特征标签
//    val addfeatuelabel = addFeatureLabel(clusterresult,bound,spark,sql)
//    //结果检验
//    val resultshow = checkResultString(addfeatuelabel ,spark)
////    val resultshow = checkResult(addfeatuelabel ,spark)
//    //保存数据
//    saveDF(clusterresult,properties)
//
//  }
//
//  def addFeatureLabel(data:DataFrame,bound:List[Double],spark:SparkSession,sql:SQLContext)={
//    //"IP","PROTO","FLOW","TAG","LABEL","DISTANCE"
//    val feature = List("PROTO","FLOW")
//    val featurebound = feature.zip(bound)
//    val label = data.select("LABEL").distinct().rdd.map(_(0).toString).collect()
//    val alllabel =label.map{ eachlabel=>
//      val tempfeature: List[String] = featurebound.map{ eachfeature=>
//        val condition = data.filter(s"LABEL=$eachlabel").select(eachfeature._1).rdd.map(_(0).toString.toDouble).mean()
//        var ret = ""
//        if(condition>eachfeature._2){ret = "upper"}
//        else{ret = "under"}
//        ret
//      }
//      (eachlabel,tempfeature.mkString("_"))
//    }
////    alllabel.foreach(println)
////    sql.createDataFrame(alllabel).toDF("LABEL","ADD_FEATURE").show(false)
//    val label_feature = sql.createDataFrame(alllabel).toDF("LABEL","ADD_FEATURE")
//    label_feature.createOrReplaceTempView("labelfeature")
//    data.createOrReplaceTempView("oridata")
//    val result = spark.sql("select t.*,s.ADD_FEATURE from oridata t left join labelfeature s on t.LABEL = s.LABEL")
//    result.show(10,false)
//    result
//  }
//
//  //获取本地数据
//  def getNetflowDataFromHbaseLocal(sql:SQLContext)={
//    val originalDataPath = "hdfs://172.16.12.26:9000/spark/data/netflowbat02"
//    val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> originalDataPath)//"hdfs://172.16.12.38:9000/spark/moniflowHDFS/2017-04-13")//modeldong
//    val dataall = sql.read.options(options).format("com.databricks.spark.csv").load()
//    dataall
//  }
//
//  //获取hbase数据
//  def getNetflowDataFromHbaseSec(properties: Properties,spark:SparkSession)={
//    val tablename = properties.getProperty("hbase.table.name")
//    val zokhost = properties.getProperty("hbase.zokhost")
//    val zokport = properties.getProperty("hbase.zokport")
//    val colnames = List("SRCIP","PROTO","PACKERNUM","BYTESIZE","RECORDTIME")
//    val data = readHbase(tablename,colnames,spark,zokhost,zokport)
//    data
//  }
//
//  //获取hbase数据
//  def getNetflowDataFromHbase(sql:SQLContext,spark:SparkSession,properties: Properties,useProperties:Boolean = false) = {
//    println("start:readindg data from hbase>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getNowTime())
//    val sc = spark.sparkContext
//    println("dealing：hbase start connecting")
//    val conf = HBaseConfiguration.create()
//    //设置zooKeeper集群地址
//    conf.set("hbase.zookeeper.quorum", properties.getProperty("hbase.zookeeper.quorum"))
//    //设置zookeeper连接端口，默认2181
//    conf.set("hbase.zookeeper.property.clientPort", properties.getProperty("hbase.zookeeper.property.clientPort"))
//    conf.set(TableInputFormat.INPUT_TABLE, "T_SIEM_NETFLOW")
//    //表名
//    //判断是否使用配置文件上的时间
//    var start = ""
//    var end = ""
//    if (useProperties) {
//      start = properties.getProperty("read.hbase.start.time")
//      end = properties.getProperty("read.hbase.end.time")
//    }
//    else {
//      start = getEndTime(-31)
//      end = getNowTime()
//    }
//    println("periods of time : from " + getEndTime(-31) + " until " + getNowTime())
//    val scan = new Scan()
//    scan.setTimeRange(Timestamp.valueOf(start).getTime, Timestamp.valueOf(end).getTime)
//    scan.setCaching(10000)
//    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
//
//    import spark.implicits._
//    val hbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
//    val data = hbaseRDD.map(row => {
//      val col1 = row._2.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("SRCIP"))
//      val col2 = row._2.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("PROTO"))
//      val col3 = row._2.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("PACKERNUM"))
//      val col4 = row._2.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("BYTESIZE"))
//      val col5 = row._2.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("RECORDTIME"))
//      val flag = if (col1 == null || col2 == null || col3 == null || col4 == null || col5 == null) 0 else 1
//      (col1, col2, col3, col4,col5, flag)
//    }).filter(_._6 > 0).map(line => {
//      (
//        Bytes.toString(line._1),
//        Bytes.toString(line._2),
//        PInteger.INSTANCE.toObject(line._3).toString,
//        PInteger.INSTANCE.toObject(line._4).toString,
//        PTimestamp.INSTANCE.toObject(line._5).toString
//      )
//    }).toDF("SRCIP", "PROTO", "PACKERNUM", "BYTESIZE","RECORDTIME")
//    println("complete：readindg data from hbase >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getNowTime())
//    data
//  }
//
////  def convertScanToString(scan: Scan) = {
////    val proto = ProtobufUtil.toScan(scan)
////    Base64.encodeBytes(proto.toByteArray)
////  }
//
//  //netflow数据处理
//  def netflowDataClean(df:DataFrame,sql: SQLContext,spark:SparkSession) = {
//    println("start：data clean >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getNowTime())
//    //筛选源ip和目的ip，并转化成DF
//    df.createOrReplaceTempView("netFlowTemp")
//    import spark.implicits._
//    val df1 = sql.sql(
//      s"""SELECT SRCIP,
//         |case when PROTO ='TCP' then 1 else 0 end as TCP,
//         |case when PROTO ='UDP' then 1 else 0 end as UDP,
//         |case when PROTO = 'ICMP' then 1 else 0 end as ICMP,
//         |case when PROTO = 'GRE' then 1 else 0 end as GRE,
//         |PACKERNUM,BYTESIZE,RECORDTIME FROM netFlowTemp""".stripMargin)
//    val df2 = df1.rdd.map{row=>
//      (row.getAs[String]("SRCIP"),row.getAs[Int]("TCP"),row.getAs[Int]("UDP"),row.getAs[Int]("ICMP"),row.getAs[Int]("GRE"),row.getAs[String]("PACKERNUM").toInt,row.getAs[String]("BYTESIZE").toInt)
//    }.toDF("IP","TCP","UDP","ICMP","GRE","PACKERNUM","BYTESIZE")
//    df2.createOrReplaceTempView("netFlowTemp1")
//    //以IP分组统计
//    val str =s"""SELECT IP,sum(TCP) TCP, sum(UDP) UDP, sum(ICMP) ICMP,sum(GRE) GRE,sum(PACKERNUM) PACKERNUM,sum(BYTESIZE) BYTESIZE
//         |FROM netFlowTemp1 GROUP BY IP""".stripMargin//输出的sum是Long类型
//    val sumData: DataFrame = spark.sql(str)
//    sql.dropTempTable(s"netFlowTemp")
//    sql.dropTempTable(s"netFlowTemp1")
//    println("show the data used to build model")
//    sumData.show(10,false)
//    sumData.describe().show
//    println("complete:cleaning data >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getNowTime())
//    sumData
//  }
//
//  //聚类过程
//  def kMeansModel(dataDF: DataFrame ,spark: SparkSession) = {
//    println("start：kmeans start >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getNowTime())
//    //    val sc: SparkContext = spark.sparkContext
//    //    val sql: SQLContext = spark.sqlContext
//    //聚类
//    //设置k值
//    var k =4 //传入轮廓系数前的k
//    val numIterations: Int = 100//迭代次数
//    //判断k的取值是否适合做聚类
//    val nData: Int = dataDF.count().toInt
//    if (nData <= k & nData>2) {k = nData - 1}
//    if (nData <= 2) {k = 1}
//    if (nData > k) {k = k}
//    val kMeansModel: KMeansModel = new KMeans().setK(k).setMaxIter(numIterations).setFeaturesCol("SCLFEATURE").setPredictionCol("LABEL").fit(dataDF)//.setMaxIter(numIterations)
//    val result: DataFrame = kMeansModel.transform(dataDF)//DF
//    //离群点
//    //    println("dealing:outlier "+ getNowTime())
//    //    var target:List[Int] = Nil
//    //    (0 to k-1).toList.foreach { eachk =>
//    //      if(result.filter(s"LABEL='$eachk'").count()<2){
//    //        target = target :+ eachk
//    //        println("cluster："+eachk+" count："+result.filter(s"LABEL='$eachk'").count())
//    //      }
//    //    }
//    //    println("outlier："+target)
//    //簇中心和簇点
//    val clusterCenters = kMeansModel.clusterCenters
//    val centersList = clusterCenters.toList
//    val centersListBD = spark.sparkContext.broadcast(centersList)
//    import spark.implicits._
//    val kmeansTrain: DataFrame = result.rdd.map {
//      row =>
//        val centersListBDs: List[Vector] = centersListBD.value
//        val IP = row.getAs[String]("IP")
//        val PROTO = row.getAs[Double]("PROTO").formatted("%.6f")
//        val FLOW = row.getAs[Double]("FLOW").formatted("%.6f")
//        val TAG = row.getAs[String]("TAG")
//        val FEATURES = row.getAs[Vector]("FEATURE")
//        val SCLFEATURES = row.getAs[Vector]("SCLFEATURE")
//        var LABEL:String = row.getAs[Int]("LABEL").toString
//        //        if(target.contains(row.getAs[Int]("LABEL"))){LABEL = "other"}//给离群点打标签
//        val CENTERPOINT: Vector = centersListBDs(row.getAs[Int]("LABEL"))//通过label获取中心点坐标
//      val DISTANCE = math.sqrt(CENTERPOINT.toArray.zip(SCLFEATURES.toArray).map(p => p._1 - p._2).map(d => d * d).sum).formatted("%.6f")//距离
//        (IP,PROTO,FLOW,TAG,LABEL,DISTANCE)
//      //    }.toDF("IP","PROTO","FLOW","TAG","FEATURE","SCLFEATURE","LABEL","CENTERPOINT","DISTANCE")
//    }.toDF("IP","PROTO","FLOW","TAG","LABEL","DISTANCE")
//    println("complete：kmeans end >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getNowTime())
//    kmeansTrain
//  }
//
//  //  def calculateCenter(data:DataFrame,spark:SparkSession)={
//  //    val label = data.select("LABEL").distinct().map(_(0).toString).rdd.collect()
//  //    label.map{ each =>
//  //      val tempdata = data.filter(s"LABEL=$each").select("PROTO","FLOW","FLAG")
//  //      tempdata.select("PROTO")
//  //    }
//  //    data.rdd.map{row =>
//  //      (row.getAs[String]("LABEL"),List(row.getAs[String]("PROTO").toDouble,row.getAs[String]("FLOW").toDouble))
//  //    }
//  //  }
//
//  def sclprocess(sumData: DataFrame, spark: SparkSession)= {
//    import spark.implicits._
//    val dataST = sumData.rdd.map { line =>
//      var temp1 = line.toSeq.toArray.map(_.toString)
//      val temp2 = temp1.drop(1)//去掉IP列
//      (temp1(0), Vectors.dense(temp2.map(_.toDouble)))
//    }.toDF("IP", "FEATURES")
//    println("IP+FEATURES:")
//    dataST.show(10,false)
//    //归一化处理
//    val scaler = new MinMaxScaler().setInputCol("FEATURES").setOutputCol("SCLFEATURES").fit(dataST)
//    val sclData: DataFrame = scaler.transform(dataST)
//    println("IP+FEATURES+SCLFEATURES")
//    sclData.show(5,false)
//    val resultdata = sclData.rdd.map{row =>
//      //      row.getAs[String]("IP")
//      val feature = row.getAs[Vector]("SCLFEATURES").toArray
//      (row.getAs[String]("IP"),feature(0),feature(1),feature(2),feature(3),feature(4),feature(5))
//    }.toDF("IP","TCP","UDP","ICMP","GRE","PACKERNUM","BYTESIZE")
//    println(resultdata.schema)
//    resultdata.show(10,false)
//    resultdata
//  }
//
//  //加权合并
//  def bindByWeight(data:DataFrame,properties: Properties,spark:SparkSession)= {
//    println("start：process of weighting >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getNowTime())
//    val w1 = List("weight.proto.tcp","weight.proto.udp","weight.proto.icmp","weight.proto.gre").map(properties.getProperty(_).toDouble)
//    val w2 = List("weight.flow.packernum","weight.flow.bytesize").map(properties.getProperty(_).toDouble)
//    import spark.implicits._
//    val scldata = sclprocess(data,spark)
//    val dataweight = scldata.rdd.map { row =>
//      val ip = row.getAs[String]("IP")
//      val proto = w1(0) * row.getAs[Double]("TCP") + w1(1) * row.getAs[Double]("UDP") + w1(2) * row.getAs[Double]("ICMP") + w1(3) * row.getAs[Double]("GRE")
//      val flow = w2(0) * row.getAs[Double]("PACKERNUM") + w2(1) * row.getAs[Double]("BYTESIZE")
//      (ip, proto, flow)
//    }.toDF("IP", "PROTO", "FLOW")
//    println("complete：process of weighting >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getNowTime())
//    dataweight
//  }
//
//  //添加标签(0-低协议低流量，1-低协议高流量，2-高协议低流量，3-高协议高流量)
//  def addLabelUseMean(data:DataFrame,spark:SparkSession,sql: SQLContext)= {
//    println("start: add label>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getNowTime())
//    //计算两个特征的均值-2*标准差和均值+2*标准差
//    val colmean: List[Double] = List("PROTO","FLOW").map(data.select(_).rdd.map(_(0).toString.toDouble).mean())
//    println("calculated the mean values of proto and flow, E(proto)="+colmean(0)+"\tE(flow)="+colmean(1))
//    val colstdev: List[Double] = List("PROTO","FLOW").map(data.select(_).rdd.map(_(0).toString.toDouble).stdev())
//    println("calculated the stdev values of proto and flow, std(proto)="+colstdev(0)+"\tstd(flow)="+colstdev(1))
////    val colbound = colmean.map(row => (row-2*colstdev(0),row+2*colstdev(1)))
////    println("calculated the outlier values of proto and flow, E-2*std="+colbound(0)+"\tE+2*std="+colbound(1))
//    val colbound: List[Double] = colmean.zip(colstdev).map(row => row._1+2*row._2)
//    println("calculated the outlier values of proto and flow,proto:E-2*std="+colbound(0)+"\tflow:E+2*std="+colbound(1))
//
//    //筛选出不满足的数据
//    val protobound = colbound(0)
//    val flowbound = colbound(1)
//    import spark.implicits._
//    val outboundprotodata = data.filter(s"PROTO>$protobound").rdd.map{row =>
//      (row.getAs[String]("IP"),row.getAs[Double]("PROTO"),row.getAs[Double]("FLOW"),"high_proto")
//    }.toDF("IP", "PROTO", "FLOW","LABEL")
//    val outboundflow = data.filter(s"FLOW>$flowbound").rdd.map{row =>
//      (row.getAs[String]("IP"),row.getAs[Double]("PROTO"),row.getAs[Double]("FLOW"),"high_flow")
//    }.toDF("IP", "PROTO", "FLOW","LABEL")
//    println("proto和flow的数据：")
//    outboundprotodata.show(20,false)
//    outboundflow.show(20,false)
//
////    val outboundip = outboundprotodata.select("IP").rdd.map(_(0).toString).toDF("IP").union(outboundflow.select("IP").rdd.map(_(0).toString).toDF("IP"))
//    val outboundip = data.filter(s"PROTO<=$protobound").union(data.filter(s"FLOW<=$protobound")).select("IP").toDF()
//    outboundip.createOrReplaceTempView("boundip")
//    data.createOrReplaceTempView("addlabeldata")
//    val filterdata = spark.sql("select IP,PROTO,FLOW from addlabeldata where IP in (select IP from boundip)")
//    println("过滤IP：")
//    filterdata.show(10,false)
//    //
//    import spark.implicits._
//    val labeldata = filterdata.rdd.map{row =>
//      val ip = row.getAs[String]("IP")
//      val proto = row.getAs[Double]("PROTO")
//      val flow = row.getAs[Double]("FLOW")
//      var label = ""
//      if(proto<=colmean(0) && flow<=colmean(1)){label = "0"}
//      if(proto<=colmean(0) && flow>colmean(1)){label = "1"}
//      if(proto>colmean(0) && flow<=colmean(1)){label = "2"}
//      if(proto>colmean(0) && flow>colmean(1)){label = "3"}
//      (ip,proto,flow,label)
//    }.toDF("IP","PROTO","FLOW","TAG")
//    println("complete:add label>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getNowTime())
//    labeldata.show(10,false)
//    (labeldata,colbound)
//  }
//
//  //添加标签(中位数)
//  def addLabelUseMedian(data:DataFrame,spark:SparkSession)= {
//    println("start: add label>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getNowTime())
//    //计算两个特征的中位数
//    val colmedian = List("PROTO","FLOW").map{row =>
//      val sorted = data.select(row).rdd.map(_(0).toString.toDouble).sortBy(identity).zipWithIndex().map{
//      case (v, idx) => (idx, v)
//      }
//      val count = sorted.count()
//      val median: Double = if (count % 2 == 0) {
//        val l = count / 2 - 1
//        val r = l + 1
//        (sorted.lookup(l).head + sorted.lookup(r).head).toDouble / 2
//
//      } else sorted.lookup(count / 2).head.toDouble
//      median
//  }
//    println("calculated the mean values of proto and flow, median(proto)="+colmedian(0)+"\tmedian(flow)="+colmedian(1))
//    import spark.implicits._
//    val labeldata = data.rdd.map{row =>
//      val ip = row.getAs[String]("IP")
//      val proto = row.getAs[Double]("PROTO")
//      val flow = row.getAs[Double]("FLOW")
//      var label = ""
//      if(proto<=colmedian(0) && flow<=colmedian(1)){label = "0"}
//      if(proto<=colmedian(0) && flow>colmedian(1)){label = "1"}
//      if(proto>colmedian(0) && flow<=colmedian(1)){label = "2"}
//      if(proto>colmedian(0) && flow>colmedian(1)){label = "3"}
//      (ip,proto,flow,label)
//    }.toDF("IP","PROTO","FLOW","TAG")
//    println("complete:add label>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getNowTime())
//    labeldata.show(10,false)
//    labeldata
//  }
//
//  //归一化
//  def scaleData(data:DataFrame,spark: SparkSession)={
//    println("start:scale data >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getNowTime())
//    import spark.implicits._
//    val dataweight = data.rdd.map { row =>
//      val ip = row.getAs[String]("IP")
//      val proto = row.getAs[Double]("PROTO")
//      val flow = row.getAs[Double]("FLOW")
//      val tag = row.getAs[String]("TAG")
//      val feature = Vectors.dense(Array(proto.toDouble,flow.toDouble))
//      (ip, proto, flow, tag, feature)
//    }.toDF("IP","PROTO","FLOW","TAG","FEATURE")
//    val scaler = new MinMaxScaler().setInputCol("FEATURE").setOutputCol("SCLFEATURE").fit(dataweight)
//    val sclData: DataFrame = scaler.transform(dataweight)
//    println("complete:scale data>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getNowTime())
//    sclData
//  }
//
//  //结果检验
//  def checkResult(data:DataFrame,spark: SparkSession) = {
//    println("the statistic method of dataframe:")
//    data.stat.crosstab("TAG","LABEL").show(10,false)
//    val inputdata = data.select("TAG","LABEL").rdd.map(row=>(row.getAs[String]("TAG").toDouble,row.getAs[String]("LABEL").toDouble))
//    val metrics = new MulticlassMetrics(inputdata)
//    val confusionMatrix = metrics.confusionMatrix
//    println("confusion Matrix:")
//    println(confusionMatrix)
//    val multiclassClassificationEvaluator = new MulticlassClassificationEvaluator().setLabelCol("TAG").setPredictionCol("LABEL")
//    import spark.implicits._
//    val metricName = List("f1","weightedPrecision","weightedRecall","accuracy")
//    metricName.foreach{eachname =>
//      println(eachname + " = " + multiclassClassificationEvaluator.setMetricName(eachname).evaluate(inputdata.toDF("TAG","LABEL")))
//    }
//    println("complete:accuracy has been calculated")
//  }
//  //结果检验
//  def checkResultString(data:DataFrame,spark: SparkSession) = {
//    println("the statistic method of dataframe:")
//    data.stat.crosstab("ADD_FEATURE","LABEL").show(10,false)
//    println("complete:accuracy has been calculated")
//  }
//
//  //保存数据到HDFS
//  def saveDF(data: DataFrame, properties: Properties, reparttition: Boolean = true) = {
//    val path = properties.getProperty("clusterResultPath.netflow.new")
//    val saveOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> path)
//    if (reparttition) {
//      data.repartition(1).write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
//    } //saveMode.Append 添加
//    else {
//      data.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
//    }
//    println("结果保存在目录：" + path)
//  }
//
//  def getNowTime(): String = {
//    val format =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val time = new Date().getTime
//    format.format(time)
//  }
//
//  def getEndTime(day:Int)={
//    val now = getNowTime()
//    val cal = Calendar.getInstance
//    cal.add(Calendar.DATE, day)
//    val time: Date = cal.getTime
//    val newtime: String =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time)
//    newtime
//  }
//
//
//}
