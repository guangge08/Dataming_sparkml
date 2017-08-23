package sftpTransfer

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import com.jcraft.jsch.ChannelSftp
import scala.collection.JavaConversions.mapAsJavaMap

/**
  * Created by dong on 2017/6/2.
  */
class sendClass(properties: Properties,resultpath155:String,savepath213:String,i:Int) extends Runnable {
  override def run() {
    var flag_exsit = 0
    var targetMachine = properties.getProperty("targetMachine")
    var username = properties.getProperty("username")
    var password = properties.getProperty("password")
    var port = properties.getProperty("port")
    var imfo155 = properties.getProperty("sendflag")
    var imfo213 = properties.getProperty("saveflag")
    while (true) {
      //扫描文件
      var filepath = new File(resultpath155).listFiles //.endsWith(".csv")
      filepath.foreach { eachfile =>
        if (eachfile.toString.endsWith(".flag")) {
          flag_exsit = 1
          eachfile.delete()
        }
      }
      //传输主过程
      if (flag_exsit == 1) {
        println(s">>>>>>>>>>>>>>>>>>>>正在进行执行(155HDFS->213本地)(from>$resultpath155->to>$savepath213)>"+getNowTime()+">>>>>>>>>>>>>>>>>>>>>>>>>>>")
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
          println(s">>>>>>>>>>>>>>>>>>>>结束(155HDFS->213本地)(from>$resultpath155->to>$savepath213)>"+getNowTime()+">>>>>>>>>>>>>>>>>>>>>>>>>>>")
        }
        flag_exsit = 0
      }
      else{
        println("扫描结果：没目标文件")
      }
      Thread.sleep(900000)
    }
  }

  def getNowTime(): String = {
    val format =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time = new Date().getTime
    format.format(time)
  }


}
