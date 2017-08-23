package CFModelAnalyse

import java.util.{Calendar, Date, Properties, UUID}
import java.io.{BufferedInputStream, FileInputStream}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Properties, UUID}

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.phoenix.schema.types.{PInteger, PTimestamp}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import utils.ROWUtils

import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._

/**
  * Created by Dong on 2017/7/3.
  */
class netflowClusterFour extends dataAccess{

  def clustermain(time:Long,properties: Properties,spark: SparkSession, sql: SQLContext)={
    //阈值分组
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>分组开始："+getEndTime(0)+">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    //读取数据（HBASE）
//    val getnetflowdata = getNetflowDataFromHbase(sql,spark,properties,false)
    //读取数据（从ES）
    val getnetflowdata = getSIEMDataFromEs(spark,"es.netflow.table.name","recordtime",properties)
//    val a = test(getnetflowdata,sql,spark)

    //数据处理
    val netflowdata = netflowDataClean(getnetflowdata,sql,spark)
    //数据标准化
    val scldata = sclprocess(netflowdata, spark)
    //添加权重
    val weightdata = bindByWeight(scldata,properties,spark)
    //添加LABEL标签
    val labeldata = addLabelUseMean(weightdata,spark,sql)
    //计算中心点特征值
    val centerdata = calculateCenter(labeldata,spark,sql)
    //与上次结果进行匹配
    val comparedata = compareLastResult(centerdata,spark,sql,properties)
    //将数据标准化到入库格式
    val insertDataBaseData = scaleResult(comparedata,time,spark)

    //储存到HDFS
    saveToHDFS(insertDataBaseData,spark,properties)//插入聚类数据
    //保存到ES
    saveToES(insertDataBaseData,"es.save.cluster",properties,sql)
    //    saveToLocal()
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>分组结束："+getEndTime(0)+">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
  }


  def getNetflowData(sql: SQLContext,properties: Properties) = {
    val hdfs = properties.getProperty("hdfs.base.path")
    val netflow = properties.getProperty("netflow.hdfs.origin")
    //    val path = "hdfs://bluedon155:9000/spark/data/netflow/" + getEndTime(-1)
    //    val path = "hdfs://172.16.12.38:9000/spark/data/netflowHDFS/" + getEndTime(-1)
    val today = new SimpleDateFormat("yyyy-MM-dd").format(new Date().getTime)
    val path = hdfs + netflow + getEndTime(-1)+"--"+today
    val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> path)
    val dataall = sql.read.options(options).format("com.databricks.spark.csv").load()
    println("原始数据的日期："+getEndTime(-1)+",原始数据示例：")
    dataall.show(5)
    dataall
  }

  //获取hbase数据
  def getNetflowDataFromHbase(sql:SQLContext,spark:SparkSession,properties: Properties,useProperties:Boolean = false) = {
    println("start:readindg data from hbase>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getEndTime(0))
    val sc = spark.sparkContext
    println("dealing：hbase start connecting")
    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址
    conf.set("hbase.zookeeper.quorum", properties.getProperty("hbase.zookeeper.quorum"))
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", properties.getProperty("hbase.zookeeper.property.clientPort"))
    conf.set(TableInputFormat.INPUT_TABLE, "T_SIEM_NETFLOW")
    //表名
    //判断是否使用配置文件上的时间
    var start = ""
    var end = ""
    if (useProperties) {
      start = properties.getProperty("read.hbase.start.time")
      end = properties.getProperty("read.hbase.end.time")
    }
    else {
      start = getEndTime(-31)
      end = getEndTime(0)
    }
    println("periods of time : from " + getEndTime(-31) + " until " + getEndTime(0))
    val scan = new Scan()
    scan.setTimeRange(Timestamp.valueOf(start).getTime, Timestamp.valueOf(end).getTime)
    scan.setCaching(10000)
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    import spark.implicits._
    val hbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val data = hbaseRDD.map(row => {
      val col1 = row._2.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("SRCIP"))
      val col2 = row._2.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("PROTO"))
      val col3 = row._2.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("PACKERNUM"))
      val col4 = row._2.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("BYTESIZE"))
      val col5 = row._2.getValue(Bytes.toBytes("NETFLOW"), Bytes.toBytes("RECORDTIME"))
      val flag = if (col1 == null || col2 == null || col3 == null || col4 == null || col5 == null) 0 else 1
      (col1, col2, col3, col4,col5, flag)
    }).filter(_._6 > 0).map(line => {
      (
        Bytes.toString(line._1),
        Bytes.toString(line._2),
        PInteger.INSTANCE.toObject(line._3).toString,
        PInteger.INSTANCE.toObject(line._4).toString,
        PTimestamp.INSTANCE.toObject(line._5).toString
      )
    }).toDF("SRCIP", "PROTO", "PACKERNUM", "BYTESIZE","RECORDTIME")
    println("complete：readindg data from hbase >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getEndTime(0))
    data
  }

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }


//  //连接es获取数据
//  def getSIEMDataFromEs(spark: SparkSession, properties: Properties,useProperties:Boolean = false) = {
//    println("开始连接es获取数据")
//    //es表名
//    val tableName = properties.getProperty("es.netflow.table.name")
//    //判断是否使用配置文件上的时间
//    var start = ""
//    var end = ""
//    if(useProperties){
//      start = properties.getProperty("read.es.start.time")
//      end = properties.getProperty("read.es.end.time")
//    }
//    else{
//      start = getEndTime(-31)
//      end = getEndTime(0)
//    }
//    //抓取时间范围
//    val starttime = Timestamp.valueOf(start).getTime
//    val endtime = Timestamp.valueOf(end).getTime
//    val query = "{\"query\":{\"range\":{\"date\":{\"gte\":" + starttime + ",\"lte\":" + endtime + "}}}}"
//    //抓取数据下来
//    val rowDF = spark.esDF(tableName, query)
//    //转换时间格式
//    val timeTransUDF=org.apache.spark.sql.functions.udf(
//      (time:Long)=>{
//        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time)
//      }
//    )
//    val tableDF=rowDF.withColumn("date1",timeTransUDF(rowDF("date")))
//    //转换大小写
//    tableDF.createOrReplaceTempView("moni")
//    val DataMN = spark.sql(
//      """SELECT srcip AS SRCIP, appproto AS APPPROTO, host AS HOST, date1 AS DATE FROM moni""".stripMargin)
//    println("开始从es保存到hdfs")
//    val path = properties.getProperty("es.data.save.hdfs")
//    val saveOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> path)
//    DataMN.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
//  }


  def test(df:DataFrame,sql: SQLContext,spark:SparkSession) = {
    println("start：data clean >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getEndTime(0))
    //转换时间格式
    val timeTransUDF=org.apache.spark.sql.functions.udf(
      (time:Long)=>{
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time)
      }
    )
    val tableDF=df.withColumn("date1",timeTransUDF(df("recordtime")))
    //筛选源ip和目的ip，并转化成DF
    df.createOrReplaceTempView("netFlowTemp")
    import spark.implicits._
    val df1 = sql.sql(
      s"""SELECT proto,count(proto) FROM netFlowTemp group by proto""".stripMargin)
    df1.show(100,false)
  }




  //0:内网攻击外网 1：外网攻击内网 2：内网攻击内网 3外网攻击外网
  def netflowDataClean(df:DataFrame,sql: SQLContext,spark:SparkSession) = {
    //test
    val savePath = "hdfs://soc70:9000/spark/dongresult/test/"
    val saveOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> savePath)
    df.repartition(1).write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()


    println("start：data clean >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getEndTime(0))
    //转换时间格式
    val timeTransUDF=org.apache.spark.sql.functions.udf(
      (time:Long)=>{
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time)
      }
    )
    val tableDF=df.withColumn("date1",timeTransUDF(df("recordtime")))
//    tableDF.show(10,false)
    //筛选源ip和目的ip，并转化成DF
    df.createOrReplaceTempView("netFlowTemp")
    import spark.implicits._
    val df1 = sql.sql(
      s"""SELECT srcip SRCIP,
         |case when proto ='TCP' then 1 else 0 end as TCP,
         |case when proto ='UDP' then 1 else 0 end as UDP,
         |packernum PACKERNUM,bytesize BYTESIZE,recordtime RECORDTIME FROM netFlowTemp""".stripMargin)
    val df2 = df1.rdd.map{row=>
      (row.getAs[String]("SRCIP"),row.getAs[Int]("TCP"),row.getAs[Int]("UDP"),row.getAs[Long]("PACKERNUM").toInt,row.getAs[Long]("BYTESIZE").toInt)
    }.toDF("IP","TCP","UDP","PACKERNUM","BYTESIZE")
    df2.createOrReplaceTempView("netFlowTemp1")
    //以IP分组统计
    val str =s"""SELECT IP,sum(TCP) TCP, sum(UDP) UDP,sum(PACKERNUM) PACKERNUM,sum(BYTESIZE) BYTESIZE
                |FROM netFlowTemp1 GROUP BY IP""".stripMargin//输出的sum是Long类型
    val sumData: DataFrame = spark.sql(str)
    sql.dropTempTable(s"netFlowTemp")
    sql.dropTempTable(s"netFlowTemp1")
    println("show the data used to build model")
    sumData.show(10,false)
    sumData.describe().show
    println("complete:cleaning data >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getEndTime(0))

    //test
    val savePath2 = "hdfs://soc70:9000/spark/dongresult/test2/"
    val saveOptions2 = Map("header" -> "true", "delimiter" -> "\t", "path" -> savePath2)
    sumData.repartition(1).write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions2).save()

    sumData
  }


  def sclprocess(sumData: DataFrame, spark: SparkSession)= {
    import spark.implicits._
    val dataST = sumData.rdd.map { line =>
      var temp1 = line.toSeq.toArray.map(_.toString)
      val temp2 = temp1.drop(1)//去掉IP列
      (temp1(0), Vectors.dense(temp2.map(_.toDouble)))
    }.toDF("IP", "FEATURES")
    //    println("IP+FEATURES:")
    //    dataST.show(10,false)
    //归一化处理
    val scaler = new MinMaxScaler().setInputCol("FEATURES").setOutputCol("SCLFEATURES").fit(dataST)
    val sclData: DataFrame = scaler.transform(dataST)
    //    println("IP+FEATURES+SCLFEATURES")
    //    sclData.show(5,false)
    val resultdata = sclData.rdd.map{row =>
      //      row.getAs[String]("IP")
      val feature = row.getAs[Vector]("FEATURES").toArray
      var sclfeatures = row.getAs[Vector]("SCLFEATURES")
      //      (row.getAs[String]("IP"),feature(0),feature(1),feature(2),feature(3),feature(4),feature(5),feature(6),sclfeatures)
      //    }.toDF("IP","TCP","UDP","ICMP","GRE","TRANTIME","PACKERNUM","BYTESIZE","SCLFEATURES")
      (row.getAs[String]("IP"),row.getAs[Vector]("FEATURES"),sclfeatures)
    }.toDF("IP","FEATURES","SCLFEATURES")
    //      println(resultdata.schema)
    //    resultdata.show(10,false)
    resultdata
  }

  //加权合并
  def bindByWeight(data:DataFrame,properties: Properties,spark:SparkSession)= {
    println("start：process of weighting >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getEndTime(0))
    val w1 = List("weight.proto.tcp","weight.proto.udp").map(properties.getProperty(_).toDouble)
    val w2 = List("weight.flow.packernum","weight.flow.bytesize").map(properties.getProperty(_).toDouble)
    import spark.implicits._
    val dataweight = data.rdd.map { row =>
      val ip = row.getAs[String]("IP")
      val feature = row.getAs[Vector]("FEATURES")
      val sclfeatures = row.getAs[Vector]("SCLFEATURES").toArray
      val proto = w1(0) * sclfeatures(0) + w1(1) * sclfeatures(1)
      val flow = w2(0) * sclfeatures(2) +w2(1) * sclfeatures(3)
      (ip, feature,row.getAs[Vector]("SCLFEATURES"), proto, flow)
    }.toDF("IP","FEATURES","SCLFEATURES", "PROTO", "FLOW")
    println("complete：process of weighting >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getEndTime(0))
    dataweight
  }

  //添加标签(0-低协议低流量，1-低协议高流量，2-高协议低流量，3-高协议高流量)
  def addLabelUseMean(data:DataFrame,spark:SparkSession,sql: SQLContext)= {
    println("start: add label>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getEndTime(0))
    //计算两个特征的均值-2*标准差和均值+2*标准差
    val colmean: List[Double] = List("PROTO","FLOW").map(data.select(_).rdd.map(_(0).toString.toDouble).mean())
    println("calculated the mean values of proto and flow, E(proto)="+colmean(0)+"\tE(flow)="+colmean(1))
    val colstdev: List[Double] = List("PROTO","FLOW").map(data.select(_).rdd.map(_(0).toString.toDouble).stdev())
    println("calculated the stdev values of proto and flow, std(proto)="+colstdev(0)+"\tstd(flow)="+colstdev(1))
    //    val colbound = colmean.map(row => (row-2*colstdev(0),row+2*colstdev(1)))
    //    println("calculated the outlier values of proto and flow, E-2*std="+colbound(0)+"\tE+2*std="+colbound(1))
    val colbound: List[Double] = colmean.zip(colstdev).map(row => row._1+3*row._2)
    println("calculated the outlier values of proto and flow,proto:E-3*std="+colbound(0)+"\tflow:E+3*std="+colbound(1))

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
    println("comlete:outlier data :")
    combindoutlier.show(10,false)

    val otherip = outboundprotodata.select("IP").union(outboundflow.select("IP")).map(_(0).toString).rdd
    val outboundip = data.select("IP").rdd.map(_(0).toString).subtract(otherip).toDF("IP")
    outboundip.createOrReplaceTempView("boundip")
    data.createOrReplaceTempView("addlabeldata")
    val filterdata = spark.sql("select IP,FEATURES,SCLFEATURES,PROTO,FLOW from addlabeldata where IP in (select IP from boundip)")

    import spark.implicits._
    val labeldata = filterdata.rdd.map{row =>
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
    println("data of adding label")
    result.show(10,false)
    println("complete:add label>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getEndTime(0))
    result
  }

  //计算中心点
  def calculateCenter(data:DataFrame,spark:SparkSession,sql: SQLContext)={
    import spark.implicits._
    //计算与中心距离
    val minmax = data.select("FEATURES").rdd.map(line=>line.getAs[Vector]("FEATURES").toArray)
    val tcp = minmax.map(line=>line(0)).max()
    val udp = minmax.map(line=>line(1)).max()
    val packernum = minmax.map(line=>line(4)).max()
    val bytesize = minmax.map(line=>line(5)).max()
    val maxcenterlist = Array(tcp,udp,packernum,bytesize)

    // /正常类
    val label = List("low_low","low_high","high_low","high_high")
    val labeldata: List[DataFrame] = label.map{ line=>
      //将FEATURES字段分开
      val tempdata = data.filter {row=>
        row.getAs[String]("LABEL")==line
      }.rdd.map{row=>
        val ip = row.getAs[String]("IP")
        val feature = row.getAs[Vector]("FEATURES")
        val label = row.getAs[String]("LABEL")
        (ip,feature,row.getAs[Vector]("SCLFEATURES"),label,feature.toArray(0),feature.toArray(1),feature.toArray(2),feature.toArray(3))
      }.toDF("IP","FEATURES","SCLFEATURES","LABEL","TCP","UDP","PACKERNUM","BYTESIZE")
      //计算CENTER
      val allfeature = List("TCP","UDP","PACKERNUM","BYTESIZE")
      val allfeaturelist = allfeature.map(tempdata.select(_).map(_(0).toString.toDouble).rdd.mean())//.mkString("#")
    val sclcenter = allfeaturelist.zip(maxcenterlist).map(row=>minmaxscala(row._1,row._2)).toArray
      //加上CENTER
      tempdata.map{line=>
        val ip = line.getAs[String]("IP")
        val feature = line.getAs[Vector]("FEATURES").toArray.map(_.toInt).mkString("#")
        val label = line.getAs[String]("LABEL")
        val returnfeature = allfeaturelist.map(_.toInt).mkString("#")
        val sclfeature = line.getAs[Vector]("SCLFEATURES").toArray
        val distance = math.sqrt(sclfeature.zip(sclcenter).map(p => p._1 - p._2).map(d => d * d).sum).toString//距离
        //        (ip,feature,label,feature(0),feature(1),feature(2),feature(3),feature(4),feature(5),feature(6),allfeaturelist.mkString("#"),distance)
        //      }.toDF("IP","FEATURES","LABEL","TCP","UDP","ICMP","GRE","TRANTIME","PACKERNUM","BYTESIZE","RETURNCENTER","DISTANCE")
        (ip,feature,label,returnfeature,distance)
      }.toDF("IP","FEATURES","LABEL","RETURNCENTER","DISTANCE")
    }

    //其他类
    val otherlabel = List("other_high_proto","other_high_flow")
    val otherlabeldata = otherlabel.map{each=>
      data.filter{row=>
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
    val unionlabeldata = labeldata.tail.foldRight(labeldata.head)(unionFun)
    val unionotherlabeldata = otherlabeldata.tail.foldRight(otherlabeldata.head)(unionFun)
    println("正常类:")
    unionlabeldata.show(5,false)
    println("other类:")
    unionotherlabeldata.show(5,false)
    val uniondata = unionlabeldata.union(unionotherlabeldata)
    uniondata
  }

//
//    // /正常类
//    val label = List("low_low","low_high","high_low","high_high")
//    val labeldata: List[DataFrame] = label.map{ line=>
//      //将FEATURES字段分开
//      val tempdata = data.filter {row=>
//        row.getAs[String]("LABEL")==line
//      }.rdd.map{row=>
//        val ip = row.getAs[String]("IP")
//        val feature = row.getAs[Vector]("FEATURES")
//        val label = row.getAs[String]("LABEL")
//        (ip,feature,row.getAs[Vector]("SCLFEATURES"),label,feature.toArray(0),feature.toArray(1),feature.toArray(2),feature.toArray(3),feature.toArray(4),feature.toArray(5))
//      }.toDF("IP","FEATURES","SCLFEATURES","LABEL","TCP","UDP","ICMP","GRE","PACKERNUM","BYTESIZE")
//      //计算CENTER
//      val allfeature = List("TCP","UDP","ICMP","GRE","PACKERNUM","BYTESIZE")
//      val allfeaturelist = allfeature.map(tempdata.select(_).map(_(0).toString.toDouble).rdd.mean())//.mkString("#")
//    val sclcenter = allfeaturelist.zip(maxcenterlist).map(row=>minmaxscala(row._1,row._2)).toArray
//      //      println("allfeaturelist:"+allfeaturelist)
//      //      println("sclcenter:"+sclcenter.toList)
//      //加上CENTER
//      tempdata.map{line=>
//        val ip = line.getAs[String]("IP")
//        val feature = line.getAs[Vector]("FEATURES")
//        val label = line.getAs[String]("LABEL")
//        val sclfeature = line.getAs[Vector]("SCLFEATURES").toArray
//        //        println("insert.sclfeature:"+sclfeature.toList)
//        val a = sclfeature.zip(sclcenter)//.map(p => p._1 - p._2).map(d => d * d).sum
//        val distance = math.sqrt(sclfeature.zip(sclcenter).map(p => p._1 - p._2).map(d => d * d).sum).toString//距离
//        //        (ip,feature,label,feature(0),feature(1),feature(2),feature(3),feature(4),feature(5),feature(6),allfeaturelist.mkString("#"),distance)
//        //      }.toDF("IP","FEATURES","LABEL","TCP","UDP","ICMP","GRE","TRANTIME","PACKERNUM","BYTESIZE","RETURNCENTER","DISTANCE")
//        (ip,feature,label,allfeaturelist.mkString("#"),distance)
//      }.toDF("IP","FEATURES","LABEL","RETURNCENTER","DISTANCE")
//    }
//
//    //其他类
//    val otherlabel = List("other_high_proto","other_high_flow")
//    val otherlabeldata = otherlabel.map{each=>
//      data.filter{row=>
//        row.getAs[String]("LABEL")==each
//      }.rdd.map{line=>
//        val ip = line.getAs[String]("IP")
//        val feature = line.getAs[Vector]("FEATURES")
//        val label = line.getAs[String]("LABEL")
//        val center = "0"
//        val distance = "0"
//        (ip,feature,label,center,distance)
//      }.toDF("IP","FEATURES","LABEL","RETURNCENTER","DISTANCE")
//    }
//
//    //union数据(离群类和正常类)
//    val unionFun = (a:DataFrame, b:DataFrame) =>a.union(b).toDF
//    val unionlabeldata = labeldata.tail.foldRight(labeldata.head)(unionFun)
//    val unionotherlabeldata = otherlabeldata.tail.foldRight(otherlabeldata.head)(unionFun)
//    println("正常类:")
//    unionlabeldata.show(5,false)
//    println("other类:")
//    unionotherlabeldata.show(5,false)
//    val uniondata = unionlabeldata.union(unionotherlabeldata)
//    uniondata
//  }

  def minmaxscala(x:Double,max:Double,min:Double=0.0)={
    var result = 0.0
    if(max!=0.0){result = (x-min)/(max-min)}
    result
  }

  def compareLastResult(data:DataFrame,spark: SparkSession,sql: SQLContext,properties: Properties)={
    import spark.implicits._
    val path = properties.getProperty("cluster_last_result")
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

    if(flag){
      println("上次模型结果数据示例：")
      lastresult.show(5)
      lastresult.createOrReplaceTempView("lastresult")
      data.createOrReplaceTempView("modelresult")
      val str = "select distinct t.*,s.LABEL LAST_LABEL from modelresult t left join lastresult s on t.IP = s.IP"
      val comparelastreuslt = spark.sql(str)
      println("关联结果：")
      lastresult.show(5)
      //"IP","FEATURES","LABEL","RETURNCENTER","DISTANCE"
      result = comparelastreuslt.rdd.map{line=>
        val ip = line.getAs[String]("IP")
        val feature = line.getAs[String]("FEATURES")
        val label = line.getAs[String]("LABEL")
        val returncenter = line.getAs[String]("RETURNCENTER")
        val distance = line.getAs[String]("DISTANCE")
        val last_label = line.getAs[String]("LAST_LABEL")
        var compare = ""
        if(label == last_label){
          compare = "0"
        }
        else{
          compare = "1"
        }
        //      (ip,feature,label,returncenter,distance,last_label,compare)
        //    }.toDF("IP","FEATURES","LABEL","RETURNCENTER","DISTANCE","COMPARELASTRESULT")
        (ip,feature,label,returncenter,distance,last_label,compare)
      }.toDF("IP","FEATURES","LABEL","RETURNCENTER","DISTANCE","LAST_LABEL","COMPARE")
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
    result
  }


  def  scaleResult(data:DataFrame,time:Long,spark: SparkSession)={
    import spark.implicits._
    val result = data.rdd.map { row =>
      val recordid: String = ROWUtils.genaralROW()
      val ip:String = row.getAs[String]("IP")
      val label: String = row.getAs[String]("LABEL")
      val distance = row.getAs[String]("DISTANCE").toDouble
      val info: String = row.getAs[String]("FEATURES")
      val last_label: String = row.getAs[String]("LAST_LABEL")
      val cltime = time
      val demo:String = "TCP#UDP#packernum#bytesize"
      val center = row.getAs[String]("RETURNCENTER")
      val tcp = info.split("#")(0).toFloat
      val udp = info.split("#")(1).toFloat
      val packernum = info.split("#")(2).toFloat
      val bytesize = info.split("#")(3).toFloat
      (recordid, ip, label, distance, cltime, demo, last_label, center, tcp,udp,packernum,bytesize)
    }.toDF("row","ip","label","distance","cltime","column","last_label","centerfeature","tcp","udp","packernum","bytesize")
      //.toDF("ROW","IP","LABEL","DISTANCE","CLTIME","COLUMN","LAST_LABEL","CENTERFEATURE","TCP","UDP","ICMP","GRE","PACKERNUM","BYTESIZE")
    result
  }


  def saveToHDFS(data:DataFrame,spark: SparkSession,properties: Properties)={
    //    val savePath = "hdfs://bluedon155:9000/spark/Assetsresult/g/cluster"// + newdate//file:///home/spark/"File:///home/hadoop/dong/test"//
    //正常保存
    val savePath = properties.getProperty("cluster_result")
    val saveOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> savePath)
    data.repartition(1).write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
    //用户关联的数据保存(用于下次的last_label)
    val savePath2 = properties.getProperty("cluster_last_result")
    val saveOptions2 = Map("header" -> "true", "delimiter" -> "\t", "path" -> savePath2)
    data.repartition(1).write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions2).save()
    println("cluster数据保存HDFS成功，路径："+savePath)
  }

/*  def saveToES(data:DataFrame,sql: SQLContext,properties: Properties)= {
    val esoption = properties.getProperty("es.save.cluster")
    try {
      data.saveToEs(esoption)
      println("complete:save to ES !")
    } catch {
      case e: Exception => println("error on save to ES：" + e.getMessage)
    }
    finally {}
  }*/

  def getNowTime(): String = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time = new Date().getTime
    format.format(time)
  }

  def getEndTime(day:Int,getdata:Boolean = false)={
    val now = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date().getTime)
    //    println(now)
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, day)
    val time: Date = cal.getTime
    var newtime = ""
    if(getdata){
      newtime = new SimpleDateFormat("yyyy-MM-dd").format(time)
    }
    else{
      newtime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time)
    }
    newtime
  }


}
