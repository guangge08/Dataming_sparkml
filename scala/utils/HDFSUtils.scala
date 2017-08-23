package utils

import java.io.FileOutputStream
import java.net.URI

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.{Configuration => HDFSConfiguration}
import org.apache.hadoop.fs.{FileSystem => HDFSFileSystem, Path => HDFSPath}

/**
  * Created by duan on 2017/6/1.
  */
trait HDFSUtils {
  def delete(delSource:String)={
    val conf = new HDFSConfiguration()
    val delPath = new HDFSPath(delSource)
    val del = HDFSFileSystem.get(URI.create(delSource), conf)
    del.delete(delPath)
  }
  def merge(srcPath:String,savePath:String)={
    //建立hdfs连接
    val conf = new HDFSConfiguration()
    //源文件夹
    val srcDir = new HDFSPath(srcPath)
    //目的文件
    val dstFile = new HDFSPath(savePath)
    //源文件系统
    val src = HDFSFileSystem.get(URI.create(srcPath), conf)
    //目的文件系统
    val dst = HDFSFileSystem.getLocal(conf).getRawFileSystem

    //取出原文件夹里的文件
    val inputFiles= src.listStatus(srcDir).filter(flie=>flie.isFile&&flie.getLen > 0)
    //创建输出流
    val out = new FileOutputStream(savePath,true)

    for(i<-0 until inputFiles.length){
      try{
        //打开本地输入流
        val in = src.open(inputFiles(i).getPath())
        if(i==0) {
          //读取全部文件
          val withtitle = IOUtils.readLines(in)
          IOUtils.writeLines(withtitle,null, out)
        }
        else {
          //去除title
          val rawlines = IOUtils.readLines(in)
          val lines = rawlines.subList(1, rawlines.size)
          IOUtils.writeLines(lines, null, out)
        }
        //释放资源
        in.close()
      }
      catch {
        case e:Exception=>
          println("输入流错误")
          e.printStackTrace()
      }
    }
    //释放资源
    out.close()
  }
}
