package utils

import java.io.{File, Serializable}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.elasticsearch.spark.sql._
/**
  * Created by duan on 2017/7/17.
  */
object dataPrepareES extends ConfSupport with LogSupport with timeSupport{
  val directory = new File("..")
  val filePath = directory.getAbsolutePath //设定为上级文件夹 获取绝对路径
  PropertyConfigurator.configure(filePath + "/conf/log4j.properties")//读取日志配置文件
  def main(args: Array[String])= {
    //初始化 配置文件和spark
    loadconf("/tableSchedule.conf")
    val sparkconf = new SparkConf()
      .setMaster(getconf("GetDataBYES.master"))
      .setAppName(getconf("GetDataBYES.appname"))
      .set("spark.port.maxRetries", getconf("GetDataBYES.portmaxRetries"))
      .set("spark.cores.max", getconf("GetDataBYES.coresmax"))
      .set("spark.executor.memory", getconf("GetDataBYES.executormemory"))
      .set("es.nodes",getconf("GetDataBYES.esnodes"))
      .set("es.port",getconf("GetDataBYES.esport"))
    val spark = SparkSession.builder.config(sparkconf).getOrCreate

    try {
      log.error("<<<<<<<<<<<<<<<<<<<<<<<预处理任务开始于"+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)+">>>>>>>>>>>>>>>>>>>>>>")
      //读取当前时间范围
      val time=getStartEndTime(args)
      //读取数据的索引
      val netflowIndex=getconf("GetDataBYES.netflowIndex")
      val moniflowIndex=getconf("GetDataBYES.moniflowIndex")
      val DPBE = new DataPrepareByES(spark,time)
      try {
        DPBE.preMONIDataByES(getconf("GetDataBYES.monibasepath"),moniflowIndex)
      }catch {
        case e: Exception => {
          log.error(s"!!!!!!MONI失败!!!!!!!!!!" + e.getMessage)
        }
      }finally{
        log.error("<<<<<<<<<<<<<MONI数据保存结束于"+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)+">>>>>>>>>>>>>>>")
      }

      try {
        DPBE.preNETDataByES(getconf("GetDataBYES.netbasepath"),netflowIndex)
      }catch {
        case e: Exception => {
          log.error(s"!!!!!!NET失败!!!!!!!!!!" + e.getMessage)
        }
      }finally{log.error("<<<<<<<<<<<<<NET数据保存结束于"+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)+">>>>>>>>>>>>>>>")}
      log.error("<<<<<<<<<<<<<<<<<<<<<<<预处理任务结束于"+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)+">>>>>>>>>>>>>>>>>>>>>>")
      spark.stop
    }
    catch {
      case e: Exception => {
        log.error(s"!!!!!!流程失败!!!!!!!!!!" + e.getMessage)
      }
    }
    finally {
      spark.stop()
    }
  }
}
class DataPrepareByES(spark: SparkSession,time: timeConfig) extends LogSupport with Serializable{
  //判断流量方向
  val IPSet=List("221.176.64", "221.176.65", "221.176.68",
    "221.176.70","221.176.71","221.176.72","221.176.73",
    "221.176.74","221.176.75","221.176.76","221.176.77","221.176.78","221.176.79")
  val flowdUDF=org.apache.spark.sql.functions.udf(
    //流量方向 flowd
    (srcip:String,dstip:String)=>{
      val SIP: String = srcip.take(srcip.lastIndexOf('.'))
      val DIP: String = dstip.take(dstip.lastIndexOf('.'))
      var ts = 0
      var td = 0
      for (qs <- 0 until IPSet.length) {
        if (IPSet(qs) == SIP) {
          ts = 1 //源ip 内网
        }
      }
      for (qd <- 0 until IPSet.length) {
        if (IPSet(qd) == DIP) {
          td = 1 //目的ip 内网
        }
      }
      //0:内网攻击外网 1：外网攻击内网 2：内网攻击内网 3外网攻击外网
      val flowd=if ((ts == 1) && (td == 1)) "2" else if((ts == 0) && (td == 1)) "1" else if((ts == 1) && (td == 0)) "0" else "3"
      //val flowd="1"
      flowd
    }
  )
  def preNETDataByES(netbasepath:String,netflowIndex:String)={
    val path = netbasepath + "/" + time.day
    log.error("<<<<<<<<<<<<<<<<<<<<<<<NETHDFS路径>>>>>>>>>>>>>>>>>>>>>>")
    log.error(path)
    log.error(">>>>>>>>>>>>>>>>预处理之前>>>>>>>>>>>>>")
    //设定时间范围
    val query="{\"query\":{\"range\":{\"recordtime\":{\"gte\":"+time.startTime/1000+",\"lte\":"+time.endTime/1000+"}}}}"
    val rawDF=spark.esDF(netflowIndex,query)
    rawDF.show(5,false)
    rawDF.printSchema()
    //转换时间格式
    val timeTransUDF=org.apache.spark.sql.functions.udf(
      (time:Long)=>{
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time*1000)
      }
    )
    val netDF=rawDF.withColumn("timeTrans",timeTransUDF(rawDF("recordtime")))
    netDF.createOrReplaceTempView("tmp")
    val Data=spark.sql(
      """
        |SELECT recordid AS ROW,proto AS PROTO,timeTrans AS RECORDTIME,(uppkts+downpkts) AS PACKERNUM,(ups+downs) AS BYTESIZE,
        |srcip AS SRCIP,dstip AS DSTIP,srcport AS SRCPORT,dstport AS DSTPORT,
        |CASE WHEN tmp.proto='tcp' THEN 1 ELSE 0 END AS TCP,
        |CASE WHEN tmp.proto='udp' THEN 1 ELSE 0 END AS UDP,
        |CASE WHEN tmp.proto='icmp' THEN 1 ELSE 0 END AS ICMP,
        |CASE WHEN tmp.proto='gre' THEN 1 ELSE 0 END AS GRE,
        |(lasttime-starttime) AS COSTTIME
        |FROM tmp
      """.stripMargin)
    //加入流量方向列
    val RESData=Data.withColumn("FLOWD",flowdUDF(Data("SRCIP"),Data("DSTIP")))
    log.error(">>>>>>>>>>>>>>>>预处理之后>>>>>>>>>>>>>")
    RESData.show(5,false)
    if(RESData.take(10).length!=0){
      val saveOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> path)
      RESData.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
    }
  }
  def preMONIDataByES(monibasepath:String,moniflowIndex:String)={
    val path = monibasepath + "/" + time.day
    log.error("<<<<<<<<<<<<<<<<<<<<<<<MONIHDFS路径>>>>>>>>>>>>>>>>>>>>>>")
    log.error(path)
    log.error(">>>>>>>>>>>>>>>>预处理之前>>>>>>>>>>>>>")
    //设定时间范围
    val query="{\"query\":{\"range\":{\"date\":{\"gte\":"+time.startTime+",\"lte\":"+time.endTime+"}}}}"
    val moniDF=spark.esDF(moniflowIndex,query)
    moniDF.show(5,false)
    moniDF.printSchema()
    moniDF.createOrReplaceTempView("moni")
    //小写转化为大写
    val DataMN=spark.sql(
      """
        |SELECT id AS ROW,date AS DATE,srcip AS SRCIP,dstip AS DSTIP,appproto AS APPPROTO,
        |srcport AS SRCPORT,dstport AS DSTPORT,method AS METHOD,uri AS URI,host AS HOST
        |FROM moni
      """.stripMargin)
    //加入流量方向列
    val RESDataMN=DataMN.withColumn("FLOWD",flowdUDF(DataMN("SRCIP"),DataMN("DSTIP"))).filter("FLOWD=1")
    log.error(">>>>>>>>>>>>>>>>预处理之后>>>>>>>>>>>>>")
    RESDataMN.show(5,false)
    if(RESDataMN.take(10).length!=0){
      val saveOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> path)
      RESDataMN.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
    }
  }
}


//val IPSetBD =spark.sparkContext.broadcast(
//List("221.176.64", "221.176.65", "221.176.68",
//"221.176.70","221.176.71","221.176.72","221.176.73",
//"221.176.74","221.176.75","221.176.76","221.176.77","221.176.78","221.176.79"))
//val RESData=netDF.mapPartitions{
//hbaseIt=>{
//val IPSet=IPSetBD.value
//hbaseIt.map {
//row =>
//var rowkey=row.getAs[String]("row")
//var starttime=row.getAs[String]("starttime")
//var endtime=row.getAs[String]("endtime")
//var proto=row.getAs[String]("proto")
//var packernum=row.getAs[String]("packernum")
//var bytesize=row.getAs[String]("bytesize")
//var srcip=row.getAs[String]("srcip")
//var dstip=row.getAs[String]("dstip")
//var recordtime=row.getAs[String]("recordtime")
//var srcport=row.getAs[String]("srcport")
//var dstport=row.getAs[String]("dstport")
//
//val costtime=if(endtime=="-"||starttime=="-") "-1" else (Timestamp.valueOf(endtime).getTime-Timestamp.valueOf(starttime).getTime).toString
////协议处理 tcp udp icmp gre
//var tcp="0"
//var udp="0"
//var icmp="0"
//var gre="0"
//proto match {
//case "TCP" =>tcp="1"
//case "UDP" =>udp="1"
//case "ICMP" =>icmp="1"
//case "GRE" =>gre="1"
//case _ => 0
//}
//
////包数量 包字节数 packernum bytesize
//
////流量方向 flowd
//val SIP: String = srcip.take(srcip.lastIndexOf('.'))
//val DIP: String = dstip.take(dstip.lastIndexOf('.'))
//var ts = 0
//var td = 0
//if (IPSet.contains(SIP)) {
//ts = 1 //源ip 内网
//}
//if (IPSet.contains(DIP)) {
//td = 1 //目的ip 内网
//}
////            0:内网攻击外网 1：外网攻击内网 2：内网攻击内网 3外网攻击外网
//val flowd=if ((ts == 1) && (td == 1)) "2" else if((ts == 0) && (td == 1)) "1" else if((ts == 1) && (td == 0)) "0" else "3"
////            val flowd="1"
//(costtime,tcp,udp,icmp,gre,proto,packernum,bytesize,flowd,rowkey,recordtime,srcip,dstip,srcport,dstport)
//}
//}
//}.toDF("COSTTIME","TCP","UDP","ICMP","GRE","PROTO","PACKERNUM","BYTESIZE","FLOWD","ROW","RECORDTIME","SRCIP","DSTIP","SRCPORT","DSTPORT")