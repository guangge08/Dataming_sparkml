package CFModelAnalyse


import java.io.Serializable
import java.sql.Timestamp

import com.github.nscala_time.time.Imports.{DateTime, DateTimeFormat}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}
import java.sql.Timestamp

import com.github.nscala_time.time.Imports._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.phoenix.schema.types.PTimestamp
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import utils.ROWUtils
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._





/**
  * Created by huoguang on 2017/5/5.
  */
class CFDataCleanProcess extends Serializable with dataAccess{
  //数据处理主函数
  def cleanData(sql: SQLContext, spark:SparkSession,properties: Properties, time:Long,limitOption:Boolean = false) = {
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>数据处理开始:" + getEndTime(0) + ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    //从HBase读取数据
//    val data =  getSIEMDataFromHbase(sql,spark,properties,false)
    //从ES读取数据并保存到HDFS
    getDataFromEs(spark, properties, false)
    //从HDFS读取数据
    val data = getSIEMDataFromHDFS(sql,properties)
    //正则匹配域名
    val dataCleanHost = cleanHost(data,sql,limitOption)
    //添加时间标签(返回带有时间标签的数据和没有时间标签的数据)
    val dataAddTime = addTimeLabel(dataCleanHost,spark)
    //按照IP/HOST分组（返回到有时间标签和不带时间标签的分组）
    val List(lastdata,lastdataNoTime) = dataGroup(dataAddTime,sql,spark)
    //标准化入库格式（使用无时间标签的数据）
    val outputdata = scaleData(lastdataNoTime,time,spark)
    //保存统计后的数据(HDFS)
    val statDataPath = properties.getProperty("statDataPath")
    saveToHDFS(outputdata,statDataPath)
    //保存统计后的数据(ES)
//    val statDataESPath = properties.getProperty("es.save.cf.stats")
    saveToES(outputdata,"es.save.cf.stats",properties,sql)
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>数据处理结束:" + getEndTime(0) + ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    (lastdata,lastdataNoTime)
  }


  def dataGroup(data:DataFrame,sql: SQLContext,spark:SparkSession)={
    import spark.implicits._
    data.createOrReplaceTempView("splitdata")
    //含有时间的分组
    val dataMorningResult = spark.sql(s"select IP, HOST,TIME, COUNT(HOST) AS COUNT from splitdata GROUP BY IP, HOST,TIME")
    println("完成：按IP、HOST、时间段的分组统计...")
    val lastdata = dataMorningResult.rdd.map { row =>
      (row.getAs[String]("IP"), row.getAs[String]("HOST"), row.getAs[String]("TIME"), row.getAs[Long]("COUNT").toDouble)
    }.toDF("IP", "HOST", "TIME", "COUNT")
    //不含时间的分组
    val dataMorningResultNoTime = spark.sql(s"select IP, HOST, COUNT(HOST) AS COUNT from splitdata GROUP BY IP, HOST")
    println("完成：按IP、HOST的分组统计...")
    val lastdataNoTime = dataMorningResultNoTime.rdd.map { row =>
      (row.getAs[String]("IP"), row.getAs[String]("HOST"), row.getAs[Long]("COUNT").toDouble)
    }.toDF("IP", "HOST", "COUNT")
    List(lastdata,lastdataNoTime)
  }

  def addTimeLabel(data:DataFrame,spark:SparkSession)={
    //根据日期划分数据
    import spark.implicits._
    val splitdata = data.rdd.map { row =>
      var date = row.getAs[String]("DATE")
      var hour = 0
      //设置初始time
      var time = "0"
      try {
        //因原始从HBASE读取数据，是根据入库时间戳读取，延迟8小时，这里进行纠正
//        val oridate = row.getAs[String]("DATE").substring(0, 19)
//        //.replace(" ","T")
//        val tempdate = DateTime.parse(oridate, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")) - 8.hour
//        date = tempdate.toString().substring(0, 19).replace("T", " ")
//        hour = date.substring(11, 13).toInt
        hour = row.getAs[String]("DATE").substring(11, 13).toInt
      } finally {
        if (hour >= 8 && hour < 12) {
          time = "morning"
        }
        if (hour >= 12 && hour < 14) {
          time = "noon"
        }
        if (hour >= 14 && hour < 18) {
          time = "afternoon"
        }
        if (hour >= 18 && hour < 24) {
          time = "night"
        }
        if (hour >= 0 && hour < 8) {
          time = "midnight"
        }
      }
      (row.getAs[String]("IP"), row.getAs[String]("TIMEMARK"), date, hour, time) //,row.getAs[String]("DATE")
    }.toDF("IP", "HOST", "DATE", "HOUR", "TIME")
    println("完成：处理时间延迟和添加时间标签...")
    splitdata.show(10,false)
    splitdata
  }

  def cleanHost(data:DataFrame,sql: SQLContext,limitOption:Boolean)={
    data.createOrReplaceTempView("oridata")
    //  正则匹配表达式：提取IP地址，或者后三位域名
    val keyFeature: String = ", REGEXP_EXTRACT(HOST, '((^[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}$)|([\\\\w]*?\\.[\\\\w]+?\\.[\\\\w]+?$))') AS TIMEMARK "
    val limitSentence = if (limitOption) " LIMIT 1000" else ""
    val df1 = sql.sql(s"select SRCIP IP" + keyFeature + ",DATE from oridata where APPPROTO='HTTP'" + limitSentence)
    df1
  }


  def scaleData(data:DataFrame,time:Long,spark:SparkSession)={
    //加入需要的字段输出
    import spark.implicits._
    val outputdata = data.rdd.map { row =>
      val recordid: String = ROWUtils.genaralROW()
      val datatype: String = "STATS"
      val cltime= time
      (recordid, row.getAs[String]("IP"), row.getAs[String]("HOST"), row.getAs[Double]("COUNT").toFloat,datatype,cltime)
    }.toDF("row","ip", "behav", "scores","data_type","cltime")
      //.toDF("ROW","IP", "BEHAV", "SCORES","DATA_TYPE","CLTIME")
    outputdata
  }

  //读取moniflow数据
  def getMoniflowData(sql: SQLContext, originalDataPath: String) = {
    val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> originalDataPath)//"hdfs://172.16.12.38:9000/spark/moniflowHDFS/2017-04-13")//modeldong
    val dataall = sql.read.options(options).format("com.databricks.spark.csv").load()
    dataall
  }


  //连接es获取数据
  def getDataFromEs(spark: SparkSession, properties: Properties,useProperties:Boolean = false) = {
    val rowDF = getSIEMDataFromEs(spark,"es.moniflow.table.name","date",properties)
    //转换时间格式
    val timeTransUDF=org.apache.spark.sql.functions.udf(
      (time:Long)=>{
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time)
      }
    )
    val tableDF=rowDF.withColumn("date1",timeTransUDF(rowDF("date")))
    //转换大小写
    tableDF.createOrReplaceTempView("moni")
    val DataMN = spark.sql(
      """SELECT srcip AS SRCIP, appproto AS APPPROTO, host AS HOST, date1 AS DATE FROM moni""".stripMargin)
    println("开始从es保存到hdfs")
    val path = properties.getProperty("es.data.save.hdfs")
    val saveOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> path)
    DataMN.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
  }

  def getSIEMDataFromHDFS(sql:SQLContext,properties: Properties)={
    val path = properties.getProperty("es.data.save.hdfs")
    val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> path)
    val data = sql.read.options(options).format("com.databricks.spark.csv").load()
    data
  }

  def getSIEMDataFromHbase(sql:SQLContext,spark:SparkSession,properties: Properties,useProperties:Boolean = false) = {
    val sc = spark.sparkContext
    println("状态：hbase连接开始")
    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址
    //conf.set("hbase.zookeeper.quorum","hadoop110")
    conf.set("hbase.zookeeper.quorum",properties.getProperty("hbase.zookeeper.quorum"))
    //设置zookeeper连接端口，默认2181
    //conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.property.clientPort", properties.getProperty("hbase.zookeeper.property.clientPort"))
    conf.set(TableInputFormat.INPUT_TABLE,"T_MONI_FLOW")//表名
    //判断是否使用配置文件上的时间
    var start = ""
    var end = ""
    if(useProperties){
      start = properties.getProperty("read.hbase.start.time")
      end = properties.getProperty("read.hbase.end.time")
    }
    else{
      start = getEndTime(-31)
      end = getEndTime(0)
    }
    println("选择数据时间段: from "+getEndTime(-31)+ " until "+ getEndTime(0))
//    val start=Timestamp.valueOf(getEndTime()).getTime
//    val end=Timestamp.valueOf(getNowTime()).getTime
    val scan = new Scan()
    //    scan.setStartRow(Bytes.toBytes("1000021490287656"))
    //    scan.setStopRow(Bytes.toBytes("1004641489851462"))
    scan.setTimeRange(Timestamp.valueOf(start).getTime,Timestamp.valueOf(end).getTime)
    scan.setCaching(10000)
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    import spark.implicits._
    val hbaseRDD = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    val data = hbaseRDD.map(row => {
      val col1 = row._2.getValue(Bytes.toBytes("FLOW"), Bytes.toBytes("SRCIP"))
      val col2 = row._2.getValue(Bytes.toBytes("FLOW"), Bytes.toBytes("APPPROTO"))
      val col3 = row._2.getValue(Bytes.toBytes("FLOW"), Bytes.toBytes("HOST"))
      val col4 = row._2.getValue(Bytes.toBytes("FLOW"), Bytes.toBytes("DATE"))
      val flag=if(col1==null||col2==null||col3==null||col4==null) 0 else 1
      (col1,col2,col3,col4,flag)
    }).filter(_._5 > 0).map(line =>{
      (
        Bytes.toString(line._1),
        Bytes.toString(line._2),
        Bytes.toString(line._3),
        PTimestamp.INSTANCE.toObject(line._4).toString
      )
    }).toDF("SRCIP","APPPROTO","HOST","DATE")
    //保存到数据文件
//    import spark.implicits._
//    val saveOptions = Map("header" -> "true","delimiter"->"\t", "path" -> "hdfs://10.252.47.211:9000/spark/eventHDFS")
//    rdd.toDF().write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
    println("状态：读取hbase数据成功!!")
    data.show(false)
    data
  }

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }


//  def getEndTime()={
//    val now = getNowTime()
//    val tempdate = DateTime.parse(now, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")) - 31.days
//    val newtime = tempdate.toString().replace("T"," ").substring(0, 19)
//    newtime
//  }

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



//  def getNowTime(): String = {
//    val format =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val time = new Date().getTime
//    format.format(time)
//  }

  def saveToHDFS(data:DataFrame,path:String)={
    val saveOption = Map("header" -> "true", "delimiter" -> "\t", "path" -> path)
    data.repartition(1).write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOption).save()//.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOption).save()//result文件夹不能创建
    println("complete:save to HDFS，on "+path)
  }


//  def saveToES(data:DataFrame,sql: SQLContext,indexs:String)= {
//    try {
//      data.saveToEs(indexs)
//      println("complete:save to ES,on "+indexs)
//    } catch {
//      case e: Exception => println("error on save to ES：" + e.getMessage)
//    } finally {}
//  }


}
