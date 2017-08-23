package utils

import java.io.FileOutputStream

import org.apache.commons.io.IOUtils
import org.apache.spark.sql.DataFrame

/**
  * Created by duan on 2017/6/13.
  */
trait SaveUtils {
  def saveAsCSV(savePath:String,data:DataFrame)={
    val out = new FileOutputStream(savePath,true)
    IOUtils.write(data.columns.mkString("\t")+"\n",out)
    data.foreachPartition{
      part=>
        val out = new FileOutputStream(savePath,true)
        part.foreach{
          line =>
          IOUtils.write(line.mkString("\t")+"\n",out)
        }
        out.close()
    }
  }
  def saveAsJSON(savePath:String,data:DataFrame)={
    data.toJSON.foreachPartition{
      part=>
        val out = new FileOutputStream(savePath,true)
        IOUtils.write(part.mkString("\n"),out)
        out.close()
    }
  }
}
