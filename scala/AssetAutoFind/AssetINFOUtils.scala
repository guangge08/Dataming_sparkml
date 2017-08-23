package AssetAutoFind

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Properties, UUID}

import org.apache.spark.sql._
import org.elasticsearch.spark.sql._
import utils.{IPUtils, sqlConfig, timeSupport}

import scala.util.Try
/**
  * Created by duan on 2017/7/28.
  */
trait AssetINFOUtils extends timeSupport with IPUtils with Serializable{

  //抓取范围内所有的访问记录
  def getDateByScan(spark: SparkSession,args: Array[String]) = {
    //获取时间范围
    val (start, end) = getTimeRangeOneHour(args)
    val query = "{\"query\":{\"range\":{\"recordtime\":{\"gte\":" + start + ",\"lte\":" + end + "}}}}"
    val rowDF = spark.esDF("netflow/netflow",query)
    //转换时间格式
    val sqlContext = spark.sqlContext
//    sqlContext.udf.register("rd",(n:Int)=>{n>98})
    val timeTransUDF = sqlContext.udf.register("timeTransUDF", (time: Long) => {new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time)})
//    val timeTransUDF = org.apache.spark.sql.functions.udf(
//      (time: Long) => {
//        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time)
//      }
//    )
    val netDF = rowDF.withColumn("timeTrans", timeTransUDF(rowDF("recordtime")))
    netDF.createOrReplaceTempView("netflow")
    //转换大小写
    val dataNET = spark.sql(
      """
        |SELECT srcip,dstip,CAST(srcport as int),CAST(dstport as int),proto,
        |CAST(packernum as int),timeTrans as recordtime
        |FROM netflow
      """.stripMargin)
    dataNET
  }


  //获得资产库以及已经被发现的IP
 def getNeIp(spark: SparkSession,sqlConfigs:sqlConfig) = {
   import spark.implicits._
   val sql_DEV: String = "(SELECT DEVICE_IP AS IP FROM T_MONI_DEVICE_IP WHERE STATUS !=2 ) AS devip"
   val sql_FIND="(SELECT IP FROM T_SIEM_DEV_FIND WHERE FLG !=1 ) findip"
//   资产库IP
   val jdbcDF_DEV = spark.read
     .format("jdbc")
     .option("driver", "org.postgresql.Driver")
     .option("url", sqlConfigs.url)
     .option("dbtable", sql_DEV)
     .option("user",sqlConfigs.username)
     .option("password",sqlConfigs.password)
     .load()
//已经被发现IP
   val jdbcDF_FIND = spark.read
     .format("jdbc")
     .option("driver", "org.postgresql.Driver")
     .option("url", sqlConfigs.url)
     .option("dbtable", sql_FIND)
     .option("user", sqlConfigs.username)
     .option("password", sqlConfigs.password)
     .load()
   jdbcDF_DEV.union(jdbcDF_FIND).map(_.getAs[String]("ip")).collect()
  }
  //筛选新发现资产
  def getNewAsset(spark: SparkSession,netflowDF: DataFrame,neips: Array[String])={
//    过滤出源IP在网段内且未被发现的 而且不是ICMP的
    val srcDF=netflowDF.filter(flow=>{
      val srcip=flow.getAs[String]("srcip")
      val srcFlag=(isIPSet(srcip)) && !(neips.contains(srcip)) && (flow.getAs[String]("proto")!="ICMP")
      srcFlag
    })
//    过滤出目的IP在网段内且未被发现 而且 是TCP而且包数量大于2 而且不是ICMP的
    val dstDF=netflowDF.filter(flow=>{
      val dstip=flow.getAs[String]("dstip")
      val dstFlag=(isIPSet(dstip)) && !(neips.contains(dstip))
      val TCPFlag=(flow.getAs[String]("proto")=="TCP")&&(Try(flow.getAs[String]("packernum").toDouble>2).getOrElse(false))
      dstFlag && TCPFlag
    })
    (srcDF,dstDF)
  }
  //转换字段名，入库
  def saveNewAsset(spark: SparkSession,ipDF:(Dataset[Row], Dataset[Row]),sqlConfigs:sqlConfig)={
//    带上ID
    val idudf=org.apache.spark.sql.functions.udf(
      ()=>{
        UUID.randomUUID().toString
      }
    )
//    时间格式转换 string转成timestamp
val timeudf=org.apache.spark.sql.functions.udf(
  (stringTime:String)=>{
    Timestamp.valueOf(stringTime)
  }
)
    ipDF._1.withColumn("storagetime",timeudf(ipDF._1("recordtime"))).createOrReplaceTempView("addsrc")
    //转换字段名 并且找出发现ip的最新时间的记录  根据源ip发现的
    val srcDF=spark.sql(
      """
        |SELECT ip,storagetime,1 AS isnew,0 AS flg,
        |max(protocol) AS protocol,max(port) AS port,max(packagenum) AS packagenum
        |FROM (SELECT  srcnet.srcip AS ip,
        |srcnet.storagetime,srcnet.proto AS protocol,srcnet.srcport AS port,srcnet.packernum AS packagenum
        |FROM addsrc AS srcnet,(SELECT srcip,max(recordtime) AS maxtime FROM addsrc GROUP BY srcip) AS srctmp
        |WHERE srcnet.srcip=srctmp.srcip AND srcnet.recordtime=srctmp.maxtime)
        |AS srcres GROUP BY srcres.ip,srcres.storagetime
      """.stripMargin)

    ipDF._2.withColumn("storagetime",timeudf(ipDF._2("recordtime"))).createOrReplaceTempView("adddst")
    //转换字段名 并且找出发现ip的最新时间的记录  根据目的ip发现的
    val dstDF=spark.sql(
      """
        |SELECT ip,storagetime,1 AS isnew,0 AS flg,
        |max(protocol) AS protocol,max(port) AS port,max(packagenum) AS packagenum
        |FROM (SELECT  dstnet.dstip AS ip,
        |dstnet.storagetime,dstnet.proto AS protocol,dstnet.dstport AS port,dstnet.packernum AS packagenum
        |FROM adddst AS dstnet,(SELECT dstip,max(recordtime) AS maxtime FROM adddst GROUP BY dstip) AS dsttmp
        |WHERE dstnet.dstip=dsttmp.dstip AND dstnet.recordtime=dsttmp.maxtime)
        |AS dstres GROUP BY dstres.ip,dstres.storagetime
      """.stripMargin)
    //入库
    val postgprop = new Properties()
    postgprop.put("user",sqlConfigs.username)
    postgprop.put("password",sqlConfigs.password)
    srcDF.withColumn("recordid",idudf()).write.mode(SaveMode.Append).jdbc(sqlConfigs.url,"T_SIEM_DEV_FIND", postgprop)
    dstDF.withColumn("recordid",idudf()).write.mode(SaveMode.Append).jdbc(sqlConfigs.url,"T_SIEM_DEV_FIND", postgprop)
  }
}
