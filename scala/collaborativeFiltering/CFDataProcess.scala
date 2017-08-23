package collaborativeFiltering

import org.apache.spark.sql._

/**
  * Created by Administrator on 2017/4/25 0025.
  */
object CFDataProcess {
  def getData(sql: SQLContext, originalDataPath: String, storePath: String, overwriteOrNot: Boolean= true, limitOption:Boolean = false):DataFrame={
    if (overwriteOrNot){
      //直接重写
      cleanData(sql, originalDataPath, storePath, limitOption)
    }else{
      //判断存储路径是否已经存在，若存在则说明已经保存了处理结果
      val existOption = new java.io.File(storePath).exists
      println(existOption)///
      if (existOption) {
        val readOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> storePath)//true要写成字符串
        //      val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> "file:///G://landun//蓝基//moniflowData_from_hadoop//20170413")//"hdfs://172.16.12.38:9000/spark/moniflowHDFS/2017-04-13")//modeldong
        val dataall = sql.read.options(readOptions).format("com.databricks.spark.csv").load()
        dataall.show
        dataall

      } else cleanData(sql, originalDataPath,storePath, limitOption)
    }

  }


  def cleanData(sql: SQLContext, originalDataPath: String, storePath: String, limitOption:Boolean = false) = {
    //元数据中读取IP, HOST，并统计COUNT

    val data: DataFrame = getMoniflowData(sql, originalDataPath)
    println("data processing:")
    data.show()
    data.createOrReplaceTempView("oridata")
    //netflow数据处理
    //flowd标签解释：0:内网攻击外网(srcIP 为内网，destIP 为外网） 1：外网攻击内网 2：内网攻击内网 3外网攻击外网
    //???还没有判断截取写法正确与否

//  正则匹配表达式：提取IP地址，或者后三位域名
    val keyFeature: String = ", REGEXP_EXTRACT(HOST, '((^[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}$)|([\\\\w]*?\\.[\\\\w]+?\\.[\\\\w]+?$))') AS TIMEMARK "
//    val keyFeature = ", Host TIMEMARK "//这是保留原来的HOST 为timeMark
    val limitSentence =  if (limitOption) " LIMIT 10" else  ""



    val df1 = sql.sql(s"select SRCIP IP" + keyFeature + " from oridata where APPPROTO='HTTP'and FLOWD='0' OR FLOWD='2'" + limitSentence)
    println("before union")
    df1.show()
    val df2 = df1.union(df1)
    println("after union")
    df2.show()
//    df2.show(100)
    sql.dropTempTable(s"oridata")
    df2.createOrReplaceTempView("beforeMerge")
    val lastdata = sql.sql(s"select IP, TIMEMARK HOST, COUNT(TIMEMARK)/2 AS COUNT from beforeMerge GROUP BY IP, TIMEMARK ")
//    lastdata.toDF("IP","TIMEMARK", "COUNT")//不用这一句
    val saveOption = Map("header" -> "true", "delimiter" -> "\t", "path" -> storePath)
    lastdata.show()

//    lastdata.persist()
    lastdata.write.options(saveOption).format("com.databricks.spark.csv").mode(SaveMode.Overwrite).save//result文件夹不能创建
    //mode, mode(saveMode.Overwrite) 每次都覆盖， saveMode.Append. 每次都添加一点点
    println("lastdata processing:")
    lastdata.show()
    lastdata

  }
  //读取moniflow数据
  def getMoniflowData(sql: SQLContext, originalDataPath: String) = {
//    val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> "hdfs://172.16.12.38:9000/spark/moniflowHDFS/2017-04-13")//"hdfs://172.16.12.38:9000/spark/moniflowHDFS/2017-04-13")//modeldong
    val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> originalDataPath)//"hdfs://172.16.12.38:9000/spark/moniflowHDFS/2017-04-13")//modeldong
    val dataall = sql.read.options(options).format("com.databricks.spark.csv").load()
    dataall
  }

}
