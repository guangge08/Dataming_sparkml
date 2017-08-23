package utils

import java.io._
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.hadoop.hbase.client.Scan
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Try


/**
  * Created by duan on 2017/4/10.
  */
object dataPrepare extends ConfSupport with LogSupport{
  val directory = new File("..")
  val filePath = directory.getAbsolutePath //设定为上级文件夹 获取绝对路径
  PropertyConfigurator.configure(filePath + "/conf/log4j.properties")//读取日志配置文件
  def main(args: Array[String])= {
    //初始化 配置文件和spark
    loadconf("/tableSchedule.conf")
    val sparkconf = new SparkConf()
      .setMaster(getconf("GetDataBYHbase.master"))
      .setAppName(getconf("GetDataBYHbase.appname"))
      .set("spark.port.maxRetries",getconf("GetDataBYHbase.portmaxRetries"))
      .set("spark.cores.max",getconf("GetDataBYHbase.coresmax"))
      .set("spark.executor.memory",getconf("GetDataBYHbase.executormemory"))
    val spark = SparkSession.builder.config(sparkconf).getOrCreate

    //zookeeper配置
    val zokhost=getconf("GetDataBYHbase.zokhost")
    val zokport=getconf("GetDataBYHbase.zokport")

    //获得取得数据的起止时间
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, -1)
    val time = cal.getTime
    val yestoday = Try(args(0).trim).getOrElse(new SimpleDateFormat("yyyy-MM-dd").format(time))
    val today = Try(args(1).trim).getOrElse(new SimpleDateFormat("yyyy-MM-dd").format(new Date))
    log.error(yestoday+"|||"+today)
    val startTime = Timestamp.valueOf(yestoday + " 00:00:00").getTime
    val endTime = Timestamp.valueOf(today + " 00:00:00").getTime
    val scan = new Scan()
    scan.setTimeRange(startTime,endTime)
    scan.setCaching(1000)
    scan.setBatch(1000)
    try {
      log.error("<<<<<<<<<<<<<<<<<<<<<<<预处理任务开始于"+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)+">>>>>>>>>>>>>>>>>>>>>>")
      //读取当前配置文件
      val DPBH = new DataPrepareByHbase(spark,zokhost,zokport,yestoday+"--"+today)
      try {
        DPBH.preMONIDataByHbase(getconf("GetDataBYHbase.monibasepath"),scan)
      }catch {
        case e: Exception => {
          log.error(s"!!!!!!MONI失败!!!!!!!!!!" + e.getMessage)
        }
      }finally{
          log.error("<<<<<<<<<<<<<MONI数据保存结束于"+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)+">>>>>>>>>>>>>>>")
      }

      try {
      DPBH.preNETDataByHbase(getconf("GetDataBYHbase.netbasepath"),scan)
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




class DataPrepareByHbase(spark: SparkSession,zokhost:String,zokport:String,day:String) extends HbaseAPI with LogSupport with Serializable{
  def preNETDataByHbase(netbasepath:String,scan:Scan)={
    import spark.implicits._
    val path = netbasepath + "/" + day
    log.error("<<<<<<<<<<<<<<<<<<<<<<<NETHDFS路径>>>>>>>>>>>>>>>>>>>>>>")
    log.error(path)
    val netDF=readHbase("T_SIEM_NETFLOW",List("STARTTIME","ENDTIME","PROTO","PACKERNUM","BYTESIZE","SRCIP","DSTIP","RECORDTIME","ROW","SRCPORT","DSTPORT"),spark,zokhost,zokport,0,scan)
    log.error(">>>>>>>>>>>>>>>>预处理之前>>>>>>>>>>>>>")
    netDF.show(5,false)
    val IPSetBD =spark.sparkContext.broadcast(
      List("221.176.64", "221.176.65", "221.176.68",
        "221.176.70","221.176.71","221.176.72","221.176.73",
        "221.176.74","221.176.75","221.176.76","221.176.77","221.176.78","221.176.79"))
    val RESData=netDF.mapPartitions{
      hbaseIt=>{
        val IPSet=IPSetBD.value
        hbaseIt.map {
          row =>
            var rowkey=row.getAs[String]("ROW")
            var starttime=row.getAs[String]("STARTTIME")
            var endtime=row.getAs[String]("ENDTIME")
            var proto=row.getAs[String]("PROTO")
            var packernum=row.getAs[String]("PACKERNUM")
            var bytesize=row.getAs[String]("BYTESIZE")
            var srcip=row.getAs[String]("SRCIP")
            var dstip=row.getAs[String]("DSTIP")
            var recordtime=row.getAs[String]("RECORDTIME")
            var srcport=row.getAs[String]("SRCPORT")
            var dstport=row.getAs[String]("DSTPORT")

            val costtime=if(endtime=="-"||starttime=="-") "-1" else (Timestamp.valueOf(endtime).getTime-Timestamp.valueOf(starttime).getTime).toString
            //协议处理 tcp udp icmp gre
            var tcp="0"
            var udp="0"
            var icmp="0"
            var gre="0"
            proto match {
              case "TCP" =>tcp="1"
              case "UDP" =>udp="1"
              case "ICMP" =>icmp="1"
              case "GRE" =>gre="1"
              case _ => 0
            }

            //包数量 包字节数 packernum bytesize

            //流量方向 flowd
            val SIP: String = srcip.take(srcip.lastIndexOf('.'))
            val DIP: String = dstip.take(dstip.lastIndexOf('.'))
            var ts = 0
            var td = 0
              if (IPSet.contains(SIP)) {
                ts = 1 //源ip 内网
              }
              if (IPSet.contains(DIP)) {
                td = 1 //目的ip 内网
              }
//            0:内网攻击外网 1：外网攻击内网 2：内网攻击内网 3外网攻击外网
            val flowd=if ((ts == 1) && (td == 1)) "2" else if((ts == 0) && (td == 1)) "1" else if((ts == 1) && (td == 0)) "0" else "3"
//            val flowd="1"
            (costtime,tcp,udp,icmp,gre,proto,packernum,bytesize,flowd,rowkey,recordtime,srcip,dstip,srcport,dstport)
        }
      }
    }.toDF("COSTTIME","TCP","UDP","ICMP","GRE","PROTO","PACKERNUM","BYTESIZE","FLOWD","ROW","RECORDTIME","SRCIP","DSTIP","SRCPORT","DSTPORT")
    log.error(">>>>>>>>>>>>>>>>预处理之后>>>>>>>>>>>>>")
    RESData.show(5,false)
    if(RESData.take(10).length!=0){
      val saveOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> path)
      RESData.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
    }
  }
  def preMONIDataByHbase(monibasepath:String,scan:Scan)={
    import spark.implicits._
    val log=Logger.getLogger(this.getClass)
    log.error("<<<<<<<<<<<<<<<<<<<<<<<zookeeper.host为"+zokhost+">>>>>>>>>>>>>>>>>>>>>>")
    log.error("<<<<<<<<<<<<<<<<<<<<<<<zookeeper.port为"+zokport+">>>>>>>>>>>>>>>>>>>>>>")

    val path = monibasepath + "/" + day
    log.error("<<<<<<<<<<<<<<<<<<<<<<<MONIHDFS路径>>>>>>>>>>>>>>>>>>>>>>")
    log.error(path)
    val moniDF=readHbase("T_MONI_FLOW",List("ROW","DATE","SRCIP","DSTIP","APPPROTO","METHOD","SRCPORT","DSTPORT","URI","HOST"),spark,zokhost,zokport,0,scan)
    log.error(">>>>>>>>>>>>>>>>预处理之前>>>>>>>>>>>>>")
    moniDF.show(5,false)
    val IPSetBD =spark.sparkContext.broadcast(
      List("221.176.64", "221.176.65", "221.176.68",
        "221.176.70","221.176.71","221.176.72","221.176.73",
        "221.176.74","221.176.75","221.176.76","221.176.77","221.176.78","221.176.79"))
    var RESDataMN=moniDF.mapPartitions{
      hbaseIt=>{
        val IPSet=IPSetBD.value
        hbaseIt.map {
          row =>
            var rowkey=row.getAs[String]("ROW")
            var date=row.getAs[String]("DATE")
            var srcip=row.getAs[String]("SRCIP")
            var dstip=row.getAs[String]("DSTIP")
            var appproto=row.getAs[String]("APPPROTO")
            var srcport=row.getAs[String]("SRCPORT")
            var dstport=row.getAs[String]("DSTPORT")
            var method=row.getAs[String]("METHOD")
            var uri=row.getAs[String]("URI")
            var host=row.getAs[String]("HOST")
            //应用协议处理 "HTTP", "QQ", "FTP", "TELNET", "DBAUDIT", "BDLOCALTROJAN"
            var http="0"
            var qq="0"
            var ftp="0"
            var telnet="0"
            var dbaudit="0"
            var bdlocal="0"
            appproto match {
              case "HTTP" =>http="1"
              case "QQ" =>qq="1"
              case "FTP" =>ftp="1"
              case "TELNET" =>telnet="1"
              case "DBAUDIT" =>dbaudit="1"
              case "BDLOCALTROJAN" =>bdlocal="1"
              case _=>0
            }
            //请求方式处理 "POST", "GET"
            var post="0"
            var get="0"
            method match {
              case "POST" =>post="1"
              case "GET" =>get="1"
              case _=>0
            }
            //流量方向 flowd
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
//            0:内网攻击外网 1：外网攻击内网 2：内网攻击内网 3外网攻击外网
            val flowd=if ((ts == 1) && (td == 1)) "2" else if((ts == 0) && (td == 1)) "1" else if((ts == 1) && (td == 0)) "0" else "3"
//            val flowd="1"

    (http,qq,ftp,telnet,dbaudit,bdlocal,appproto,get,post,method,srcip,dstip,srcport,dstport,host,uri,rowkey,date,flowd)
  }
}
}.toDF("HTTP", "QQ", "FTP", "TELNET", "DBAUDIT", "BDLOCALTROJAN", "APPPROTO","GET","POST","METHOD", "SRCIP", "DSTIP", "SRCPORT", "DSTPORT", "HOST", "URI", "ROW", "DATE","FLOWD")
    log.error(">>>>>>>>>>>>>>>>预处理之后>>>>>>>>>>>>>")
    RESDataMN.show(5,false)
    if(RESDataMN.take(10).length!=0){
      val saveOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> path)
      RESDataMN.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
    }
  }
}