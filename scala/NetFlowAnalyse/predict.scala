package NetFlowAnalyse
import java.io._
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, sql}
import utils._
/**
  * Created by duan on 2017/5/11.
  */
object predict extends ConfSupport with HDFSUtils{
  val directory = new File("..")
  val filePath = directory.getAbsolutePath //设定为上级文件夹 获取绝对路径
  PropertyConfigurator.configure(filePath + "/conf/log4j.properties")
  val log=Logger.getLogger(this.getClass)
  def main(args: Array[String]) {
    //加载配置文件
    loadconf("/models.conf")
    val sparkconf = new SparkConf()
      .setMaster(getconf("netflow.master"))
      .setAppName(getconf("netflow.appname"))
      .set("spark.port.maxRetries",getconf("netflow.portmaxRetries"))
      .set("spark.cores.max",getconf("netflow.coresmax"))
      .set("spark.executor.memory",getconf("netflow.executormemory"))
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    val sc=spark.sparkContext

    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, -1)
    val time = cal.getTime
    val yestoday = new SimpleDateFormat("yyyy-MM-dd").format(time)
    val today = new SimpleDateFormat("yyyy-MM-dd").format(new Date)
    val respath=getconf("netflow.res_path")
    val filename="T_SIEM_NETFLOW_RFPREDICT@"+yestoday+"@NET"

    try {
      log.error("<<<<<<<<<<<<<<<<<<<<<<<流量分析预测任务开始于"+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)+">>>>>>>>>>>>>>>>>>>>>>")
      val path=getconf("netflow.netflow_path") + "/" + yestoday+"--"+today
//      val path=getconf("netflow.netflow_path") + "/" + "2017-05-27"
      log.error("<<<<<<<<<<<<<待检测数据路径>>>>>>>>>>>>>>>")
      log.error(path)
      val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> path)
      val alldata = spark.read.options(options).format("com.databricks.spark.csv").load().limit(1000000)
      log.error("<<<<<<<<<<<<<数据读取成功>>>>>>>>>>>>>>>")
      alldata.createOrReplaceTempView("netflowtmp")
      val time1 = System.currentTimeMillis()
      //COSTTIME,TCP,UDP,PROTO,PACKERNUM,BYTESIZE,SRCIP,DSTIP,ROW,RECORDTIME,SRCPORT,DSTPORT,FLOWD,FLAG
      val predictdata = spark.sql("SELECT * from netflowtmp ORDER BY RECORDTIME,SRCIP,DSTIP,PROTO")
      predictdata.persist(StorageLevel.MEMORY_AND_DISK)
      val time2 = System.currentTimeMillis()
      log.error("<<<<<<<<<<<<<排序成功>>>>>>>>>>>>>>>")
      log.error("排序用时为" + (time2 - time1) / 1000 + "秒")
      val model=PipelineModel.load(getconf("netflow.modelpp_path"))
      log.error("<<<<<<<<<<<<<模型导入成功>>>>>>>>>>>>>>>")
      val time3 = System.currentTimeMillis()
      val RunPredict = new RunPredict(model,spark,respath+filename)
      RunPredict.Predict(predictdata)
      //最后给出flag
      val time4 = System.currentTimeMillis()
      log.error("<<<<<<<<<<<<<检测全部成功>>>>>>>>>>>>>>>")
      log.error("检测用时为" + (time4 - time3) / 1000 + "秒")
      log.error("<<<<<<<<<<<<<<<<<<<<<<<流量分析预测任务结束于"+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)+">>>>>>>>>>>>>>>>>>>>>>")
      spark.stop()
    }catch {
      case e: Exception => {
        log.error(s"!!!!!!流量分析预测流程失败!!!!!!!!!!" + e.getMessage)
      }
    }
    finally {
      sc.stop()
      spark.stop()
    }
  }
}
class RunPredict(model: PipelineModel,spark:SparkSession,respath:String) extends Serializable with SaveUtils{
  import spark.implicits._
  def Predict(data: sql.DataFrame) = {
    //预先转换字段类型 划定检测范围
    var preDS=datapre(data)
    //检测
    val resDF=model.transform(preDS).filter("prediction > 0")
    //入库之前的处理
    val endDS=dataend(resDF)
    //结果入库
    if(endDS.take(10).length>0) {
      saveResult(endDS.distinct)
    }
  }

  //预先转换字段类型 划定检测范围
  def datapre(data: sql.DataFrame) ={
    //筛选出外网对内网的 而且一秒内有重复数据流的
    //    .filter("FLAG != '0'")
    var preDS=data.repartition(24).mapPartitions{
      PartLine => {
        var flowLabel=new Array[String](2)
        flowLabel=Array("xxx","aaa")
        //现在的标识 之前的标识
        PartLine.map{
          line =>
            val RECORDTIME=line.getAs[String]("RECORDTIME")
            val SRCIP=line.getAs[String]("SRCIP")
            val DSTIP=line.getAs[String]("DSTIP")
            val PROTO=line.getAs[String]("PROTO")
            flowLabel(0) =
              RECORDTIME ++  //现在的标识
                SRCIP ++
                DSTIP++
                PROTO

            //获取检测目标
            val target=if(flowLabel(0) == flowLabel(1)&&line.getAs[String]("FLOWD")=="1") 1 else 0

            //转换数据类型
            val rawline=RAWNET(
              SPANDTIME = line.getAs[String]("COSTTIME").toDouble,
              TCP = line.getAs[String]("TCP").toDouble,
              UDP = line.getAs[String]("UDP").toDouble,
              PACKERNUM = line.getAs[String]("PACKERNUM").toDouble,
              BYTESIZE = line.getAs[String]("BYTESIZE").toDouble,
              PROTO = line.getAs[String]("PROTO"),
              SRCIP = line.getAs[String]("SRCIP"),
              DSTIP = line.getAs[String]("DSTIP"),
              RECORDTIME = line.getAs[String]("RECORDTIME"),
              ROW = line.getAs[String]("ROW"),
              SRCPORT = line.getAs[String]("SRCPORT"),
              DSTPORT = line.getAs[String]("DSTPORT"),
              target = target
            )

            flowLabel(1) =
              RECORDTIME ++  //之前的的标识
                SRCIP ++
                DSTIP++
                PROTO

            rawline
        }
      }
    }.filter(_.target>0)
    preDS
  }
  //最后入库之前改变一下字段类型
  def dataend(resDF: Dataset[Row]) ={
    //持久化 为后面复杂的逻辑做准备
    resDF.persist(StorageLevel.MEMORY_AND_DISK)
    resDF.createOrReplaceTempView("tmp")
    //统计同流数量
    val countDF=spark.sql(
      """SELECT tmp.prediction,tmp.PACKERNUM,tmp.PROTO,tmp.ROW,tmp.SRCIP,
        |tmp.DSTIP,tmp.SPANDTIME,tmp.RECORDTIME,tmp.BYTESIZE,tmp.SRCPORT,tmp.DSTPORT,res.FLOWNUMS
        |FROM tmp,
        |(SELECT SRCIP,DSTIP,PROTO,RECORDTIME,COUNT(*) AS FLOWNUMS FROM tmp GROUP BY SRCIP,DSTIP,PROTO,RECORDTIME) AS res
        |WHERE tmp.DSTIP=res.DSTIP AND tmp.SRCIP=res.SRCIP AND tmp.PROTO=res.PROTO AND tmp.RECORDTIME=res.RECORDTIME""".stripMargin)
    val endDS=countDF.mapPartitions {
      PartLine => {
        PartLine.map {
          line =>
            val prediction=line.getAs[Double]("prediction")
            val proto=line.getAs[String]("PROTO")
            val PACKERNUM=line.getAs[Double]("PACKERNUM")
            val attacktype=(prediction,proto,PACKERNUM) match {
              case (1.0,"TCP",_)=>"CC"
              case (2.0,"UDP",_)=>"UDPFlood"
              case (2.0,"TCP",_)=>"SYNFlood"
              case (3.0,"TCP",_)=>"SYNFlood"
              case (3.0,"UDP",_)=>"UDPFlood"
              case (4.0,_,1.0)=>"PORTSCAN"
              case _=>""
            }
            val resline=RESNET(
              SRCIP = line.getAs[String]("SRCIP"),
              DSTIP =line.getAs[String]("DSTIP"),
              ATTACKTYPE = attacktype,
              RECORDTIME = line.getAs[String]("RECORDTIME"),
              PROTO = line.getAs[String]("PROTO"),
              SRCPORT = line.getAs[String]("SRCPORT"),
              DSTPORT = line.getAs[String]("DSTPORT"),
              FLOWNUMS = line.getAs[Long]("FLOWNUMS").toInt
            )
            resline
        }
      }
    }.filter(line=>(line.ATTACKTYPE!="")&&(line.FLOWNUMS>10))
    endDS
  }
  def saveResult(resDS: Dataset[RESNET]) = {
    resDS.createOrReplaceTempView("res")
    val endDF=spark.sql(
      """
        |SELECT ATTACKTYPE,SRCIP,DSTIP,PROTO,SRCPORT,DSTPORT,RECORDTIME,FLOWNUMS,COUNT(*) AS FLOWNUMA
        |FROM res GROUP BY ATTACKTYPE,SRCIP,DSTIP,PROTO,SRCPORT,DSTPORT,RECORDTIME,FLOWNUMS ORDER BY RECORDTIME,ATTACKTYPE,SRCIP,DSTIP,PROTO,SRCPORT,DSTPORT,FLOWNUMS
      """.stripMargin
      )
    //.filter(line=>(line.getAs[String]("ATTACKTYPE")=="PORTSCAN" && line.getAs[String]("FLOWNUMA")=="1")||(line.getAs[String]("FLOWNUMA")!="1"))
    endDF.show(5,false)
    if(endDF.take(10).length>0) {
      saveAsCSV(respath + ".csv", endDF)
      val file = new File(respath + ".flag").createNewFile()
    }
  }
}




//    val saveoptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> path)
//    endDF.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveoptions).save()





//endDS.distinct.foreachPartition{
//  PartLine => {
//  //插入到hbase
//  //插入到sql
//  val log=Logger.getLogger(this.getClass)
//  val db=new DBUtils
//  val hbconn=db.getPhoniexConnect(postgprop.getProperty("zookeeper.host")+":"+postgprop.getProperty("zookeeper.port"))
//  val sqlconn=db.getSQLConnect(sqlurl=postgprop.getProperty("aplication.sql.url"),username=postgprop.getProperty("aplication.sql.username"),password=postgprop.getProperty("aplication.sql.password"))
//  hbconn.setAutoCommit(false);sqlconn.setAutoCommit(false)
//  val prephb  = hbconn.prepareStatement("UPSERT INTO t_siem_netflow_rfpredict (\"ROW\",\"SRCIP\",\"DSTIP\",\"ATTACKTYPE\",\"RECORDTIME\",\"COSTTIME\",\"PROTO\",\"PACKERNUM\",\"BYTESIZE\",\"FLOWDIRECTION\",\"SRCPORT\",\"DSTPORT\",\"FLOWNUMA\") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)")
//  val prepsql = sqlconn.prepareStatement("INSERT INTO t_siem_netflow_rfpredict (RECORDID,SRCIP,DSTIP,ATTACKTYPE,RECORDTIME,COSTTIME,PROTO,PACKERNUM,BYTESIZE,FLOWDIRECTION,SRCPORT,DSTPORT,FLOWNUMA) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)")
//  PartLine.foreach{
//  line =>
//  try{
//  prephb.setString(1,line.ROW);prepsql.setString(1,line.ROW)
//  prephb.setString(2,line.SRCIP);prepsql.setString(2,line.SRCIP)
//  prephb.setString(3,line.DSTIP);prepsql.setString(3,line.DSTIP)
//  prephb.setString(4,line.ATTACKTYPE);prepsql.setString(4,line.ATTACKTYPE)
//  prephb.setTimestamp(5,line.RECORDTIME);prepsql.setTimestamp(5,line.RECORDTIME)
//  prephb.setFloat(6,line.COSTTIME);prepsql.setFloat(6,line.COSTTIME)
//  prephb.setString(7,line.PROTO);prepsql.setString(7,line.PROTO)
//  prephb.setInt(8,line.PACKERNUM);prepsql.setInt(8,line.PACKERNUM)
//  prephb.setInt(9,line.BYTESIZE);prepsql.setInt(9,line.BYTESIZE)
//  prephb.setString(10,line.FLOWDIRECTION);prepsql.setString(10,line.FLOWDIRECTION)
//  prephb.setInt(11,line.SRCPORT);prepsql.setInt(11,line.SRCPORT)
//  prephb.setInt(12,line.DSTPORT);prepsql.setInt(12,line.DSTPORT)
//  prephb.setInt(13,line.FLOWNUMA);prepsql.setInt(13,line.FLOWNUMA)
//  prephb.executeUpdate;prepsql.executeUpdate
//}catch {
//  case e: Exception => {
//  log.error(s"!!!!!!插入失败!!!!!!!!!!" + e.getMessage)
//}
//}
//}
//  hbconn.commit;sqlconn.commit
//  hbconn.close;sqlconn.close
//}
//}






















//    val putTableName = "T_SIEM_NETFLOW_RFPREDICT"
//    val hbaseConf = HBaseConfiguration.create()
//    hbaseConf.set("hbase.zookeeper.quorum", postgprop.getProperty("zookeeper.host"))
//    hbaseConf.set("hbase.zookeeper.property.clientPort", postgprop.getProperty("zookeeper.port"))
//    val hBaseTable = new HTable(hbaseConf, putTableName)
//    var batch = new util.ArrayList[Row]()
//          val put = new Put(Bytes.toBytes(line(0)))
//          put.add(Bytes.toBytes("RFPREDICT"), Bytes.toBytes("SRCIP"), Bytes.toBytes(line(1)))
//          put.add(Bytes.toBytes("RFPREDICT"), Bytes.toBytes("DSTIP"), Bytes.toBytes(line(2)))
//          put.add(Bytes.toBytes("RFPREDICT"), Bytes.toBytes("ATTACKTYPE"), Bytes.toBytes(line(3)))
//          put.add(Bytes.toBytes("RFPREDICT"), Bytes.toBytes("RECORDTIME"), PTimestamp.INSTANCE.toBytes(Timestamp.valueOf(line(4))))
//          put.add(Bytes.toBytes("RFPREDICT"), Bytes.toBytes("COSTTIME"), PFloat.INSTANCE.toBytes(line(5).toDouble))
//          put.add(Bytes.toBytes("RFPREDICT"), Bytes.toBytes("PROTO"), Bytes.toBytes(line(6)))
//          put.add(Bytes.toBytes("RFPREDICT"), Bytes.toBytes("PACKERNUM"), PInteger.INSTANCE.toBytes(line(7).toDouble.toInt))
//          put.add(Bytes.toBytes("RFPREDICT"), Bytes.toBytes("BYTESIZE"), PInteger.INSTANCE.toBytes(line(8).toDouble.toInt))
//          put.add(Bytes.toBytes("RFPREDICT"), Bytes.toBytes("FLOWDIRECTION"), Bytes.toBytes(line(9)))
//          put.add(Bytes.toBytes("RFPREDICT"), Bytes.toBytes("SRCPORT"), PInteger.INSTANCE.toBytes(line(10).toDouble.toInt))
//          put.add(Bytes.toBytes("RFPREDICT"), Bytes.toBytes("DSTPORT"), PInteger.INSTANCE.toBytes(line(11).toDouble.toInt))
//          batch.add(put)
