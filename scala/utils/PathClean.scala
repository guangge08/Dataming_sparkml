package utils

import java.util.Properties

/**
  * Created by duan on 2017/2/24.
  */
class PathClean(postgprop:Properties) {
  /**
    * 判断路径是否存在
    **/
  def clean(path:String): Unit = {
    val impl: String = postgprop.getProperty("spark.save.fs.hdfs.impl")
    val output = new org.apache.hadoop.fs.Path(path)
    val conft = new org.apache.hadoop.conf.Configuration()
    conft.set("mapred.jop.tracker", postgprop.getProperty("spark.mapred.jop.tracker"))
    conft.set("fs.default.name", postgprop.getProperty("spark.fs.default.name"))
    conft.set("fs.hdfs.impl", impl)
    val hdfs = org.apache.hadoop.fs.FileSystem.get(conft)
    //     删除输出目录
    if (hdfs.exists(output)) {
      hdfs.delete(output, true)
    }
  }
}
