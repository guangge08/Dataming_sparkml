package sftpTransfer


import java.io.{BufferedInputStream, File, FileInputStream}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}
import com.jcraft.jsch.ChannelSftp
import scala.collection.JavaConversions.mapAsJavaMap
import scala.sys.process._
import com.github.nscala_time.time.Imports.{DateTime, DateTimeFormat}
import com.github.nscala_time.time.Imports._


/**
  * Created by huoguang on 2017/5/18.
  */
object getfrom213 {

  def main(args: Array[String]): Unit ={
    var flag = true
    while(flag){
      flag = getFile(flag)
      Thread.sleep(900000)
    }
  }

  def getFile(flag:Boolean) = {
    //读取配置文件
    val filePath = System.getProperty("user.dir")
    val properties: Properties = new Properties()
    val ipstream = new BufferedInputStream(new FileInputStream(filePath + "/conf/getfrom213.properties"))
    properties.load(ipstream)
    val targetMachine = properties.getProperty("targetMachine")
    val username = properties.getProperty("username")
    val password = properties.getProperty("password")
    val port = properties.getProperty("port")

    //SFTP连接
    var flag0 = flag
    var flag1 = false
    var flag2 = false
    var flag3 = false
    val sftpMap1: java.util.Map[String, String] = mapAsJavaMap(Map("host" -> targetMachine, "username" -> username, "password" -> password, "location" -> "",
      "port" -> port)) //如果出现了socket 没有建立，可能是链接配置出错
    val sftpChannelObject: SFTPChannel = new SFTPChannel()
    val sftp: ChannelSftp = sftpChannelObject.getChannel(sftpMap1, 10000) //传入的必须为java的Map对象,timeout.
    try {
      println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>正在进行数据远程传输(213->155)>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getNowTime())
      //      new File(properties.getProperty("save.up.155")+"netflow.tar.gz").delete()
      //      new File(properties.getProperty("save.up.155")+"moniflow.tar.gz").delete()
      //      println("成功:删除155上的旧压缩包")
      sftp.get(properties.getProperty("netflow.path.213"), properties.getProperty("save.up.155"))
      println("NETFLOW文件GET成功：远程路径>"+properties.getProperty("netflow.path.213") + " 存放在本地的路径>"+properties.getProperty("save.up.155"))
      sftp.get(properties.getProperty("moniflow.path.213"), properties.getProperty("save.up.155"))
      println("MONIFLOW文件GET成功：远程路径>"+properties.getProperty("moniflow.path.213") + " 存放在本地的路径>"+properties.getProperty("save.up.155"))
      flag1 = true
    } catch {
      case e: Exception => println("出错：通过SFTP传输数据（213>155）出错>" + e.getMessage)
    }
    finally {
      println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>数据远程传输结束(213->155)>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getNowTime())
      sftpChannelObject.closeChannel()
    } //关闭

    try {
      println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>NETFLOW数据操作>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getNowTime())
      val fcmd1 = "tar -xzvf " + properties.getProperty("save.up.155") + "/netflow.tar.gz -C " + properties.getProperty("save.netflow.155") //+getPath1
      println("开始解压："+fcmd1)
      fcmd1.!!
      println("解压成功："+fcmd1)
      val today = new SimpleDateFormat("yyyy-MM-dd").format(new Date().getTime)
      val cmd1 = "hadoop fs -put " + properties.getProperty("save.netflow.155") + getEndTime(-1) + "--" + today + " " + properties.getProperty("put.netflow.hdfs.155")
      println("开始执行命令：" + cmd1)
      cmd1.!!
      println("执行命令成功：" + cmd1)
      val dcmd1 = "hadoop fs -rm -r "+ properties.getProperty("put.netflow.hdfs.155") + getEndTime(-2)+"--"+getEndTime(-1)
      dcmd1.!!
      println("删除成功：删除HDFS上前一天的数据")
      val dlcmd1 = "rm -rf "+properties.getProperty("save.netflow.155") + getEndTime(-1)+"--"+today
      dlcmd1.!!
      println("删除成功：删除本地的今天数据")
      flag2 = true
    }catch {case e: Exception => println("出错：" + e.getMessage)
    } finally {
      println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>NETFLOW数据操作结束>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getNowTime())
    }

    try{
      println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>MONIFLOW数据操作>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getNowTime())
      val fcmd2 = "tar -xzvf "+ properties.getProperty("save.up.155") +"/moniflow.tar.gz -C "+ properties.getProperty("save.moniflow.155") //+getPath2
      println("开始解压："+ fcmd2)
      fcmd2.!!
      println("解压成功："+ fcmd2)
      val today = new SimpleDateFormat("yyyy-MM-dd").format(new Date().getTime)
      val cmd2 = "hadoop fs -put "+properties.getProperty("save.moniflow.155")+ getEndTime(-1) + "--" + today  + " " + properties.getProperty("put.moniflow.hdfs.155")
      println("开始执行命令：" + cmd2)
      cmd2.!!
      println("执行命令成功：" + cmd2)
      val dcmd2 = "hadoop fs -rm -r "+ properties.getProperty("put.moniflow.hdfs.155") + getEndTime(-2)+"--"+getEndTime(-1)
      dcmd2.!!
      println("删除成功：删除HDFS上前一天的数据")
      val dlcmd2 = "rm -rf "+properties.getProperty("save.moniflow.155") + getEndTime(-1)+"--"+today
      dlcmd2.!!
      println("删除成功：删除本地的今天数据")
      flag3 = true
    } catch {
      case e: Exception => println("出错：" + e.getMessage)
    }
    finally {
      println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>MONIFLOW数据操作结束>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getNowTime())
    }
    //上传标识文件
    try{
      println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>标识文件操作>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getNowTime())
      //      var dfcmd3 = "hadoop fs -rm "+properties.getProperty("put.flag.hdfs.155 ") + "ident.file"
      //      dfcmd3.!!
      //      println("删除上一次的ident.file成功："+dfcmd3)
      new File(properties.getProperty("save.flag.155")).mkdirs()
      new File(properties.getProperty("save.flag.155")+"ident.file").createNewFile()
      var cmd3 = "hadoop fs -put "+properties.getProperty("save.flag.155")+"ident.file"+" "+properties.getProperty("put.flag.hdfs.155")
      cmd3.!!
      println("新标识文件上传HDFS成功："+cmd3)
    }catch {
      case e: Exception => println("出错：" + e.getMessage)
    }
    finally {
      println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>标识文件操作结束>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+getNowTime())
    }
    //如果三个条件满足则返回false
    if(flag1&flag2&flag3){
      flag0 = false
    }
    flag0
  }


  def getNowTime(): String = {
    val format =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time = new Date().getTime
    format.format(time)
  }

  def getEndTime(day:Int)={
    val now = getNowTime()
    //    println(now)
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, day)
    val time: Date = cal.getTime
    val newtime: String =new SimpleDateFormat("yyyy-MM-dd").format(time)
    newtime
  }

}