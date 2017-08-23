package sftpTransfer

import java.io.File

import com.jcraft.jsch.ChannelSftp

/**
  * Created by Administrator on 2017/5/17.
  */
//import org.apache.commons.net.ftp.FTPClient
//import org.apache.commons.net.ftp.FTPReply
//import org.apache.commons.net.ftp.FTPSClient
//import com.jcraft.jsch._
//object FTPTest {
//  //这里需要设置日志
//  def main(args:Array[String]) {
//    println("Hello, world!")
//    var ftpClient= new FTPClient()
//
//    val dstHost = "172.16.1.111"
//    val SFTPPASS = "123456"
//    val targetDirPath = "/home/tempStore/"
////    val localFilePath = "/root/spark/moniflowHDFS/credStore/intc.csv"
//    val localFilePath = "D://wenjian.txt"//本地机器上的文件系统，不需要file:///
////    val localFilePath = "D://sdaf.txt"//找不到指定文件
//
//
//    System.out.println("preparing the host information for sftp.")
//
//    val jsch = new JSch()
//    var session = jsch.getSession("root", dstHost, 22)
////    LOG.debug("Session created.")
//
//    session.setPassword(SFTPPASS)
//    var config = new java.util.Properties()
//    config.put("StrictHostKeyChecking", "no")
//    session.setConfig(config)
//    session.connect()
//    System.out.println("Host connected.")
//    var channel = session.openChannel("sftp")
//
//    channel.connect()
//    System.out.println("sftp channel opened and connected.")
//
//
//    var sftpChannel: ChannelSftp = session.openChannel("sftp").asInstanceOf[ChannelSftp]//
////    System.out.println("Directory:" + sftpChannel.pwd())
////
//    try{
//      sftpChannel.cd(targetDirPath)
//
//    } catch{
//      case ex: SftpException =>{
//        sftpChannel.mkdir(targetDirPath)
//        sftpChannel.cd(targetDirPath)
//        sftpChannel.put(localFilePath, targetDirPath, 2)//写入文件
//      }
//      case _ =>
//    }finally {session.disconnect()}
//
//
//
//
//
//  }
//
//}
import scala.collection.JavaConversions._//转换scala map为java map
object SFTPSCALA {
  val targetDirPath = "/home/hadoop/dong"
  ////    val localFilePath = "/root/spark/moniflowHDFS/credStore/intc.csv"
  val localFilePath = new File("/home/dongtest/test/data").listFiles.map(_.getAbsolutePath)//本地机器上的文件系统，不需要file:///
  println("将要传输的文件："+localFilePath.toList)
//  localFilePath.foreach(println(_))


  val sftpMap1: java.util.Map[String, String] = mapAsJavaMap(Map("host" -> "172.16.1.110", "username" -> "root", "password"->"123456", "location" -> "",
    "port" ->"22"))//如果出现了socket 没有建立，可能是链接配置出错

  def main(args: Array[String]): Unit ={
    val sftpChannelObject: SFTPChannel = new SFTPChannel()//新建channel
    val sftp: ChannelSftp = sftpChannelObject.getChannel(sftpMap1,10000)//传入的必须为java的Map对象,timeout.
    localFilePath.foreach(filePath => sftp.put(filePath,targetDirPath, 0 ))//传输, 0 代表overwrite
    println("传输成功！")
    sftpChannelObject.closeChannel()//关闭
  }

}

