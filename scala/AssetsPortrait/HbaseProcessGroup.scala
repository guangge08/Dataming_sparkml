package AssetsPortrait

import java.io.{BufferedInputStream, FileInputStream, InputStream}
import java.sql.Timestamp
import java.io.File
import java.util.{Calendar, Date, Properties, UUID}

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql._
import utils.DBUtils
import utils.ROWUtils
import org.apache.hadoop.conf.Configuration
import java.net.URI
import java.text.SimpleDateFormat

import utils.HDFSUtils


import scala.sys.process._


/**
  * Created by huoguang on 2017/5/26.
  */
trait HbaseProcessGroup extends HDFSUtils{

  //一级聚类插入簇中心点表
//  def insertSIEMCenter(data:DataFrame,CLNO:String,TIME:String,TYPE:String,DEMO:String,spark: SparkSession) = {
//    val conn = new DBUtils().getPhoniexConnect("10.252.47.211,10.252.47.212,10.252.47.213,10.252.47.215:2181")//("hadoop:2181")////
//    conn.setAutoCommit(false)
//    val state = conn.createStatement()
//    //获取中心点的数据
//    data.createOrReplaceTempView("OriData")
//    val result = spark.sql("select distinct LABEL,RETURNCENTER from OriData").toDF()
//    try {
//      val tempResult = result.rdd.map { row =>
//        val recordid: String = ROWUtils.genaralROW()
//        val index: String = row.getAs[String]("LABEL")
//        val info: String = row.getAs[List[Int]]("RETURNCENTER").mkString("#")
//        val clno: String = CLNO.replace("-","")
//        val Type: String = TYPE
//        val cltime: String = TIME
//        val demo: String = DEMO
//        //s"TCP#UDP#ICMP#GRE#tftime#packernum#bytesize"
//        val dataori: String = "g"
//        List(recordid, index, info, clno, Type, cltime, demo, dataori)
//      }
//      val lastdata: Array[List[String]] = tempResult.collect()
//      println("一级聚类，插入hbase的示例数据:")
//      lastdata.take(3).foreach(println)
//      for (row <- lastdata) {
//        val sql = new StringBuilder()
//          .append("upsert into T_SIEM_KMEANS_CENTER (\"ROW\",\"CENTER_INDEX\",\"CENTER_INFO\",\"CLNO\",\"TYPE\",\"CLTIME\",\"DEMO\",\"DATA_ORIGIN\") ")
//          .append("values ('" + row(0) + "','" + row(1) + "','" + row(2) + "','" + row(3) + "'," + row(4) + ",'" + Timestamp.valueOf(row(5)) + "','" + row(6) + "','" + row(7) + "')")
//          .toString()
//        //          .append("values ('" + recordid + "','" + index + "','" + info + "','" + clno + "'," + Type + ",'"  + Timestamp.valueOf(cltime) +"','" + demo+"','" + dataori + "')" )
//        state.execute(sql)
//        conn.commit()
//      }
//      println(s"insert into hbase success!")
//    } catch {
//      case e: Exception => {
//        println(s"Insert into Hbase occur error!! " + e.getMessage)
//        conn.rollback()
//      }
//    } finally {
//      state.close()
//      conn.close()
//    }
//  }


  //聚类簇点插入到hbase
//  def insertSIEMCluster(data:DataFrame,CLNO:String,TIME:String,DEMO:String,spark: SparkSession) = {
//    val conn = new DBUtils().getPhoniexConnect("10.252.47.211,10.252.47.212,10.252.47.213,10.252.47.215:2181")//("hadoop:2181")//(//
//    conn.setAutoCommit(false)
//    val state = conn.createStatement()
//    //获取中心点的数据
//    try{
//      val result = data.rdd.map { row =>
//        val recordid: String = ROWUtils.genaralROW()
//        val name:String = row.getAs[String]("IP")
//        val index: String = row.getAs[String]("LABEL")
//        val clno: String = CLNO.replace("-","")
//        val distance:String = row.getAs[String]("DISTANCE").toString
//        val info: String = row.getAs[Vector]("FEATURES").toArray.mkString("#")
//        val cltime:String = TIME
//        val demo:String = DEMO//s"TCP#UDP#ICMP#GRE#tftime#packernum#bytesize"
//      val dataori:String = "g"
//        List(recordid, clno, name, index, distance, info, cltime, demo, dataori)
//      }
//      val lastdata: Array[List[String]] = result.collect()
//      println("聚类后簇点，插入hbase的示例数据:")
//      lastdata.take(3).foreach(println)
//      for (row <- lastdata) {
//        val sql = new StringBuilder()
//          .append("upsert into T_SIEM_KMEANS_CLUSTER (\"ROW\",\"CLNO\",\"NAME\",\"CENTER_INDEX\",\"DISTANCE\",\"CLUSTER_INFO\",\"CLTIME\",\"DEMO\",\"DATA_ORIGIN\") ")
//          .append("values ('" + row(0) + "','" + row(1) + "','" + row(2) + "','" + row(3) + "'," + row(4) + ",'" + row(5) + "','" + Timestamp.valueOf( row(6)) +"','" +  row(7)+"','" +  row(8) + "')" )
//          .toString()
//        state.execute(sql)
//        conn.commit()
//      }
//      println(s"insert into hbase success!")
//    } catch {
//      case e: Exception => {
//        println(s"Insert into Hbase occur error!! " + e.getMessage)
//        conn.rollback()
//      }
//    } finally {
//      state.close()
//      conn.close()
//    }
//  }

  def saveClusterCsv(data:DataFrame,CLNO:String,TIME:String,DEMO:String,spark: SparkSession,properties: Properties)={
    import spark.implicits._
    val result = data.rdd.map { row =>
      val recordid: String = ROWUtils.genaralROW()
      val name:String = row.getAs[String]("IP")
      val index: String = row.getAs[String]("LABEL")
      val clno: String = CLNO.replace("-","")
      val distance:String = row.getAs[String]("DISTANCE").toString
      val info: String = row.getAs[Vector]("FEATURES").toArray.map(_.toInt).mkString("#")
      val cltime:String = TIME
      val demo:String = DEMO//s"TCP#UDP#ICMP#GRE#tftime#packernum#bytesize"
      val dataori:String = "g"
      val tcp = info.split("#")(0)
      val udp = info.split("#")(1)
      val icmp = info.split("#")(2)
      val gre = info.split("#")(3)
      val tftime = info.split("#")(4)
      val packernum = info.split("#")(5)
      val bytesize = info.split("#")(6)
      (recordid, clno, name, index, distance, info, cltime, demo, dataori,tcp,udp,icmp,gre,tftime,packernum,bytesize)
    }.toDF("ROW","CLNO","NAME","CENTER_INDEX","DISTANCE","CLUSTER_INFO","CLTIME","DEMO","DATA_ORIGIN","TCP","UDP","ICMP","GRE","TFTIME","PACKERNUM","BYTESIZE")
//    val savePath = "hdfs://bluedon155:9000/spark/Assetsresult/g/cluster"// + newdate//file:///home/spark/"File:///home/hadoop/dong/test"//
    //正常保存
    val savePath = properties.getProperty("hdfs.base.path")+properties.getProperty("cluster.hdfs.path")
    val saveOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> savePath)
    result.repartition(1).write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
    //用户关联的数据保存
    val savePath2 = properties.getProperty("hdfs.base.path")+properties.getProperty("cluster.hdfs.path2")
    val saveOptions2 = Map("header" -> "true", "delimiter" -> "\t", "path" -> savePath2)
    result.repartition(1).write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions2).save()
    println("cluster数据保存HDFS成功，路径："+savePath)
  }

  def saveCenterCsv(data:DataFrame,CLNO:String,TIME:String,TYPE:String,DEMO:String,spark: SparkSession,properties: Properties)= {
    data.createOrReplaceTempView("center")
    val result = spark.sql("select distinct LABEL,RETURNCENTER from center").toDF()
    import spark.implicits._
    val tempResult = result.rdd.map { row =>
      val recordid: String = ROWUtils.genaralROW()
      val index: String = row.getAs[String]("LABEL")
      val info = row.getAs[String]("RETURNCENTER")
      val clno: String = CLNO.replace("-", "")
      val Type: String = TYPE
      val cltime: String = TIME
      val demo: String = DEMO
      //s"TCP#UDP#ICMP#GRE#tftime#packernum#bytesize"
      val dataori: String = "g"
      val templist =  info.split("#")
      var tcp: String = "0"
      var udp: String = "0"
      var icmp: String = "0"
      var gre: String = "0"
      var tftime: String = "0"
      var packernum: String = "0"
      var bytesize: String = "0"
      if(templist.length!=1){
        tcp =templist(0)
        udp = templist(1)
        icmp = templist(2)
        gre = templist(3)
        tftime = templist(4)
        packernum = templist(5)
        bytesize = templist(6)
      }
      (recordid, index, info, clno, Type, cltime, demo, dataori,tcp,udp,icmp,gre,tftime,packernum,bytesize)
    }.toDF("ROW","CENTER_INDEX","CENTER_INFO","CLNO","TYPE","CLTIME","DEMO","DATA_ORIGIN","TCP","UDP","ICMP","GRE","TFTIME","PACKERNUM","BYTESIZE")//.saveAsTextFile("center_data.csv")
//    val savePath = "hdfs://bluedon155:9000/spark/Assetsresult/g/center"// + newdate//file:///home/spark/"File:///home/hadoop/dong/test"//
    val savePath = properties.getProperty("hdfs.base.path")+properties.getProperty("center.hdfs.path")
    val saveOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> savePath)
    tempResult.repartition(1).write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
    println("center数据保存HDFS成功，路径："+savePath)
  }

  def saveClusterCsvNew(data:DataFrame,CLNO:String,TIME:String,DEMO:String,spark: SparkSession,properties: Properties)={
    import spark.implicits._
    val result = data.rdd.map { row =>
      val recordid: String = ROWUtils.genaralROW()
      val name:String = row.getAs[String]("IP")
      val index: String = row.getAs[String]("LABEL")
      val clno: String = CLNO.replace("-","")
      val distance:String = row.getAs[String]("DISTANCE").toString
      val info: String = row.getAs[String]("FEATURES")
      val last_label: String = row.getAs[String]("LAST_LABEL")
      val compare: String = row.getAs[String]("COMPARE")
      val cltime:String = TIME
      val demo:String = DEMO//s"TCP#UDP#ICMP#GRE#tftime#packernum#bytesize"
    val dataori:String = "g"
      val tcp = info.split("#")(0)
      val udp = info.split("#")(1)
      val icmp = info.split("#")(2)
      val gre = info.split("#")(3)
      val tftime = info.split("#")(4)
      val packernum = info.split("#")(5)
      val bytesize = info.split("#")(6)
      (recordid, clno, name, index, distance, info, cltime, demo, dataori, last_label, compare, tcp,udp,icmp,gre,tftime,packernum,bytesize)
    }.toDF("ROW","CLNO","NAME","CENTER_INDEX","DISTANCE","CLUSTER_INFO","CLTIME","DEMO","DATA_ORIGIN","LAST_CENTER_INDEX","COMPARE_LAST_RESULT","TCP","UDP","ICMP","GRE","TFTIME","PACKERNUM","BYTESIZE")
    //    val savePath = "hdfs://bluedon155:9000/spark/Assetsresult/g/cluster"// + newdate//file:///home/spark/"File:///home/hadoop/dong/test"//
    //正常保存
    val savePath = properties.getProperty("hdfs.base.path")+properties.getProperty("cluster.hdfs.path")
    val saveOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> savePath)
    result.repartition(1).write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
    //用户关联的数据保存
    val savePath2 = properties.getProperty("hdfs.base.path")+properties.getProperty("cluster.hdfs.path2")
    val saveOptions2 = Map("header" -> "true", "delimiter" -> "\t", "path" -> savePath2)
    result.repartition(1).write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions2).save()
    println("cluster数据保存HDFS成功，路径："+savePath)
  }

  def saveCenterCsvNew(data:DataFrame,CLNO:String,TIME:String,TYPE:String,DEMO:String,spark: SparkSession,properties: Properties)= {
    data.createOrReplaceTempView("center")
    val result = spark.sql("select distinct LABEL,RETURNCENTER from center").toDF()
    import spark.implicits._
    val tempResult = result.rdd.map { row =>
      val recordid: String = ROWUtils.genaralROW()
      val index: String = row.getAs[String]("LABEL")
      val info = row.getAs[String]("RETURNCENTER")
      val clno: String = CLNO.replace("-", "")
      val Type: String = TYPE
      val cltime: String = TIME
      val demo: String = DEMO
      //s"TCP#UDP#ICMP#GRE#tftime#packernum#bytesize"
      val dataori: String = "g"
      val templist =  info.split("#").map(_.toDouble.toInt.toString)
      var tcp: String = "0"
      var udp: String = "0"
      var icmp: String = "0"
      var gre: String = "0"
      var tftime: String = "0"
      var packernum: String = "0"
      var bytesize: String = "0"
      if(templist.length!=1){
        tcp =templist(0)
        udp = templist(1)
        icmp = templist(2)
        gre = templist(3)
        tftime = templist(4)
        packernum = templist(5)
        bytesize = templist(6)
      }
      (recordid, index, info, clno, Type, cltime, demo, dataori,tcp,udp,icmp,gre,tftime,packernum,bytesize)
    }.toDF("ROW","CENTER_INDEX","CENTER_INFO","CLNO","TYPE","CLTIME","DEMO","DATA_ORIGIN","TCP","UDP","ICMP","GRE","TFTIME","PACKERNUM","BYTESIZE")//.saveAsTextFile("center_data.csv")
    //    val savePath = "hdfs://bluedon155:9000/spark/Assetsresult/g/center"// + newdate//file:///home/spark/"File:///home/hadoop/dong/test"//
    //正常保存
    val savePath = properties.getProperty("hdfs.base.path")+properties.getProperty("center.hdfs.path")
    val saveOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> savePath)
    tempResult.repartition(1).write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
    println("center数据保存HDFS成功，路径："+savePath)
  }

  def saveToLocal()={
    val filePath = System.getProperty("user.dir")
    val properties: Properties = new Properties()
    val ipstream = new BufferedInputStream(new FileInputStream(filePath + "/conf/cluster.properties"))
    //val ipstream: InputStream = this.getClass().getResourceAsStream("/config.properties")
    properties.load(ipstream)
    println("开始HDFS->本地...")
    try {
      //从HDFS保存到本地
      val gcmd1 = "hadoop fs -get "+ properties.getProperty("center.hdfs.path") + " " + properties.getProperty("center.local.path")
      val gcmd2 = "hadoop fs -get "+ properties.getProperty("cluster.hdfs.path") + " " + properties.getProperty("cluster.local.path")
      gcmd1.!!
      println("成功:center>HDFS存放到本地"+properties.getProperty("center.local.path"))
      gcmd2.!!
      println("成功:cluster>HDFS存放到本地"+properties.getProperty("cluster.local.path"))
      //删除HDFS上的结果数据
//      try{
//        val dcmd1 = "hadoop fs -rm -r "+ properties.getProperty("center.hdfs.path")
//        dcmd1.!!
//        println("删除成功：删除HDFS的center数据")
//        val dcmd2 = "hadoop fs -rm -r "+ properties.getProperty("cluster.hdfs.path")
//        dcmd2.!!
//        println("删除成功：删除HDFS的cluster数据")
//      }catch {
//        case e: Exception => println("删除HDFS结果数据出错："+ e.getMessage)
//      }finally {}
      //保存flag文件到本地
      new File(properties.getProperty("file1.path")).createNewFile()
      new File(properties.getProperty("file2.path")).createNewFile()
      println("成功：生成标志文件")
    }catch {
      case e: Exception => println("出错："+ e.getMessage)
    }finally {
      println("结束HDFS->本地...")
    }
  }

  def HDFSToLocal(properties: Properties)= {
    val saveCenter = properties.getProperty("save.local")+properties.getProperty("hbase.table.center") +"@" +getEndTime(-1) +"@G"
    val saveCluster = properties.getProperty("save.local")+properties.getProperty("hbase.table.cluster") +"@" +getEndTime(-1) +"@G"
    merge(properties.getProperty("hdfs.base.path")+properties.getProperty("center.hdfs.path"),saveCenter+".csv")
    println("成功：CENTER HDFS->本地 成功")
    merge(properties.getProperty("hdfs.base.path")+properties.getProperty("cluster.hdfs.path"),saveCluster+".csv")
    println("成功：CLUSTER HDFS->本地 成功")
    new File(saveCenter+".flag").createNewFile()
    new File(saveCluster+".flag").createNewFile()
    println("成功：生成的标识文件")
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


