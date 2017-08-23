package sftpTransfer

import java.io.{BufferedInputStream, File, FileInputStream}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import com.jcraft.jsch.ChannelSftp
import scala.collection.JavaConversions.mapAsJavaMap

/**
  * Created by Dong on 2017/5/31.
  */
object sendto213 {

  def main(args: Array[String]): Unit = {
    //读取配置文件
    val filePath = System.getProperty("user.dir")
    val properties: Properties = new Properties()
    val ipstream = new BufferedInputStream(new FileInputStream(filePath + "/conf/sendto213.properties"))
    properties.load(ipstream)
    //配置日志文件
    //    val logger = Logger.getLogger(this.getClass)
    //    PropertyConfigurator.configure(System.getProperty("user.dir") + "/conf/log4j.properties")

    //定义155存放结果
    val resultpath155 = List(properties.getProperty("resultPathGCenter"), properties.getProperty("resultPathGCluster"),
      properties.getProperty("resultPathSCenter1"), properties.getProperty("resultPathScenter2"),
      properties.getProperty("resultPathSCluster"), properties.getProperty("resultnetflow"),
      properties.getProperty("resultmoniflow"))
    //定义213接受文件路径
    val savepath213 = List(properties.getProperty("saveGCenter213"), properties.getProperty("saveGCluster213"),
      properties.getProperty("saveSCenterone213"), properties.getProperty("saveSCentertwo213"),
      properties.getProperty("saveSCluster213"), properties.getProperty("saveNetflow213"),
      properties.getProperty("saveMoniflow213"))
    //while
    var flag_exsit = 0
    var targetMachine = properties.getProperty("targetMachine")
    var username = properties.getProperty("username")
    var password = properties.getProperty("password")
    var port = properties.getProperty("port")
    var imfo155 = properties.getProperty("sendflag")
    var imfo213 = properties.getProperty("saveflag")
    while (true) {
      println("==============================================开始扫描文件："+ getNowTime() +"================================================")
      for (i <- 0 to resultpath155.length - 1) {
        println("扫描文件路径："+resultpath155(i))
        sftpprocess(properties,resultpath155(i),savepath213(i),i)
      }
      println("==============================================扫描沉睡：" + getNowTime() +"================================================")
      Thread.sleep(900000)//15分钟
    }
  }

  def sftpprocess(properties: Properties,resultpath155:String,savepath213:String,i:Int)={
    var flag_exsit = false
    var targetMachine = properties.getProperty("targetMachine")
    var username = properties.getProperty("username")
    var password = properties.getProperty("password")
    var port = properties.getProperty("port")
    var imfo155 = properties.getProperty("sendflag")
    var imfo213 = properties.getProperty("saveflag")
    //扫描文件
    try{
      var filepath = new File(resultpath155).listFiles //.endsWith(".csv")
      filepath.foreach { eachfile =>
        if (eachfile.toString.endsWith(".flag")) {
          flag_exsit = true
          eachfile.delete()
        }
      }
    }catch{
      case e: Exception => println("扫描出错：" + e.getMessage)
    } finally {}
    //传输主过程
    if (flag_exsit) {
      println("扫描结果：目标文件存在")
      println(s">>>>>>>>>>>>>正在进行执行(155HDFS->213本地)(from>$resultpath155->to>$savepath213)>"+getNowTime()+">>>>>>>>>>>>")
      new File(imfo155+i+".flag").createNewFile()
      println("成功：标识文件创建成功")
      //SFTP连接
      var sftpMap1: java.util.Map[String, String] = mapAsJavaMap(Map("host" -> targetMachine, "username" -> username, "password" -> password, "location" -> "", "port" -> port))
      var sftpChannelObject: SFTPChannel = new SFTPChannel()
      var sftp: ChannelSftp = sftpChannelObject.getChannel(sftpMap1, 10000)
      println("成功：建立115到213的SFTP连接")
      try {
        var absputdata1 = new File(resultpath155).listFiles.map(_.getAbsolutePath)
        absputdata1.foreach(eachfile => sftp.put(eachfile, savepath213))
        println("成功：155 >>>>>> 213，文件传输成功")
        sftp.put(imfo155+i+".flag",imfo213)
        println("成功：标识文件传输成功")
        new File(resultpath155).listFiles.foreach(eachfile => eachfile.delete())
        println("成功：删除存放在155的结果数据")
      } catch {
        case e: Exception => println("出错：" + e.getMessage)
      } finally {
        sftpChannelObject.closeChannel()
        println(s">>>>>>>>>>>>结束(155HDFS->213本地)(from>$resultpath155->to>$savepath213)>"+getNowTime()+">>>>>>>>>>>>>")
      }
      flag_exsit = false
    }
    else{
      println("扫描结果：目标文件不存在的")
    }
  }

  def getNowTime(): String = {
    val format =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time = new Date().getTime
    format.format(time)
  }


}