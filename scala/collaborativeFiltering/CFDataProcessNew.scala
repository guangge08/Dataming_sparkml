package collaborativeFiltering

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.DataFrame
import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import org.apache.spark.sql.{Row, SparkSession, _}
import CFDataProcess._
import CFEnvironmentSetup._
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import CFCalculateSimilarity._
import CFCalucateRating._
import com.github.nscala_time.time.Imports._


/**
  * Created by Administrator on 2017/5/4.
  */
object CFDataProcessNew {
  def getData(sql: SQLContext,spark:SparkSession, originalDataPath: String, collaborativeResultPath: String, overwriteOrNot: Boolean= true, limitOption:Boolean = false)={
    if (overwriteOrNot){
      //直接重写
      cleanData(sql, spark,originalDataPath, collaborativeResultPath, limitOption)
    }else{
      //判断存储路径是否已经存在，若存在则说明已经保存了处理结果（直接读取处理好的结果）
      val existOption = new java.io.File(collaborativeResultPath).exists
      println(existOption)///
      if (existOption) {
        val readOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> collaborativeResultPath)//true要写成字符串
        //      val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> "file:///G://landun//蓝基//moniflowData_from_hadoop//20170413")//"hdfs://172.16.12.38:9000/spark/moniflowHDFS/2017-04-13")//modeldong
        val dataall = sql.read.options(readOptions).format("com.databricks.spark.csv").load()
//        dataall.show
        dataall
      } else cleanData(sql, spark,originalDataPath,collaborativeResultPath, limitOption)
    }
  }

  def cleanData(sql: SQLContext, spark:SparkSession,originalDataPath: String, collaborativeResultPath: String, limitOption:Boolean = false) = {
    //元数据中读取IP, HOST，并统计COUNT
    val data: DataFrame = getMoniflowData(sql, originalDataPath)
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>数据处理开始:" + getNowTime() + ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    //    data.show(false)
    data.createOrReplaceTempView("oridata")
    //flowd标签解释：0:内网攻击外网(srcIP 为内网，destIP 为外网） 1：外网攻击内网 2：内网攻击内网 3外网攻击外网
    //  正则匹配表达式：提取IP地址，或者后三位域名
    val keyFeature: String = ", REGEXP_EXTRACT(HOST, '((^[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}$)|([\\\\w]*?\\.[\\\\w]+?\\.[\\\\w]+?$))') AS TIMEMARK "
    //    val keyFeature = ", Host TIMEMARK "//这是保留原来的HOST 为timeMark
    val limitSentence = if (limitOption) " LIMIT 100" else ""
    val df1 = sql.sql(s"select SRCIP IP" + keyFeature + ",DATE from oridata where APPPROTO='HTTP'and FLOWD='0' OR FLOWD='2'" + limitSentence)
    //    df1.printSchema()
    //根据日期划分数据
    import spark.implicits._
    val splitdata = df1.rdd.map { row =>
      var date = row.getAs[String]("DATE")
      var hour = 0
      //设置初始time
      var time = "0"
      try {
        val oridate = row.getAs[String]("DATE").substring(0, 19)
        //.replace(" ","T")
        val tempdate = DateTime.parse(oridate, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")) - 8.hour
        date = tempdate.toString().substring(0, 19).replace("T", " ")
        hour = date.substring(11, 13).toInt
      }
      if (hour >= 8 && hour < 12) {
        time = "morning"
      }
      if (hour >= 12 && hour < 14) {
        time = "noon"
      }
      if (hour >= 14 && hour < 18) {
        time = "afternoon"
      }
      if (hour >= 18 && hour < 24) {
        time = "night"
      }
      if (hour >= 0 && hour < 8) {
        time = "midnight"
      }
      (row.getAs[String]("IP"), row.getAs[String]("TIMEMARK"), date, hour, time) //,row.getAs[String]("DATE")
    }.toDF("IP", "HOST", "DATE", "HOUR", "TIME")
    splitdata.show(100, false)
    splitdata.createOrReplaceTempView("splitdata")
    val dataMorningResult = sql.sql(s"select IP, HOST,TIME, COUNT(HOST) AS COUNT from splitdata GROUP BY IP, HOST,TIME")
    dataMorningResult.show(false)
    val lastdata = dataMorningResult.rdd.map { row =>
      (row.getAs[String]("IP"), row.getAs[String]("HOST"), row.getAs[String]("TIME"), row.getAs[Long]("COUNT").toDouble)
    }.toDF("IP", "HOST", "TIME", "COUNT")
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>数据处理结束:" + getNowTime() + ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    ////    val saveOption = Map("header" -> "true", "delimiter" -> "\t", "path" -> collaborativeResultPath)
    ////    lastdata.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOption).save()//result文件夹不能创建
    ////    (dataMorningResult,dataNoonResult,dataAfternoonResult,dataNightResult,dataMidnightResult)
    lastdata
  }

  //读取moniflow数据
  def getMoniflowData(sql: SQLContext, originalDataPath: String) = {
    val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> originalDataPath)//"hdfs://172.16.12.38:9000/spark/moniflowHDFS/2017-04-13")//modeldong
    val dataall = sql.read.options(options).format("com.databricks.spark.csv").load()
    dataall
  }
  def getNowTime(): String = {
    val format =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time = new Date().getTime
    format.format(time)
  }




}
