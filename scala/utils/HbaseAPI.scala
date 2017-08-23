package utils

import java.sql.Timestamp

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.log4j.Logger
import org.apache.spark.sql._

/**
  * Created by duan on 2017/3/2.
  */
trait HbaseAPI extends Serializable with PDataTypeUtils with timeSupport with LogSupport{
  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  //通配的读取方法  要特别注意的是style为0意味着输出字段均为String，为1意味着保留表中原类型
  def readHbase(tablename:String,colnames:List[String],spark:SparkSession,zokhost:String,zokport:String,style:Int=1,scan: Scan=new Scan)={
    //加载配置文件
    loadconf("/tableSchedule.conf")
    //列簇
    val colfamily=getconf(tablename + ".colsfamily")
    //字段在phoenix表中的结构
    val configs: List[ColConfig] =getColConfigs(tablename,colnames)
    //字段的数据结构
    val schema=getschema(configs,style)
    //开始连接hbase
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum",zokhost)
    conf.set("hbase.zookeeper.property.clientPort",zokport)
    conf.set(TableInputFormat.INPUT_TABLE,tablename)
    conf.set(TableInputFormat.SCAN,convertScanToString(scan))
    val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    //获取值，转化成DataFrame
    spark.createDataFrame(
      hbaseRDD.map{
        line=>
          val seq=getColValues(line,colfamily,configs,style)
          org.apache.spark.sql.Row.fromSeq(seq)
      }
      ,schema
    )
  }

  //通配的入库工具 要特别注意的是，resDF中的每个字段必须都是String类型
  def saveToHbase(resDF:DataFrame,putTableName:String,zokhost:String,zokport:String)={
    //加载配置文件
    loadconf("/tableSchedule.conf")
    //获取列簇
    val colsfamily = getconf(putTableName + ".colsfamily")
    log.error(">>>>>>>>列簇："+colsfamily)
    //获取字段名
    val colnames=resDF.columns.toList.map(_.trim)
    //字段在phoenix表中的结构
    val configs: List[ColConfig] =getColConfigs(putTableName,colnames)
    //批数量
    val size=2000
    log.error(">>>>>>>>>>>>>>>>>>>>字段配置："+configs)
    log.error(getlabeltime+">>>>>>>>>>>>>>>>>>>>>>>>>>开始写入数据<<<<<<<<<<<<<<<<<<<<<<<")
    resDF.foreachPartition {
      part=>
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set("hbase.zookeeper.quorum", zokhost)
      hbaseConf.set("hbase.zookeeper.property.clientPort", zokport)
      hbaseConf.set(HConstants.HBASE_RPC_TIMEOUT_KEY, "2000000")
      val con = ConnectionFactory.createConnection(hbaseConf)
      val hBaseTable = con.getTable(TableName.valueOf(putTableName))
      var batch = new java.util.ArrayList[org.apache.hadoop.hbase.client.Row]()
      part.foreach(
        line => {
            batch.add(getSavePut(line, configs, putTableName, colsfamily))
            if (batch.size() > size) {
              hBaseTable.batch(batch)
              batch.clear()
            }
        }
      )
      if (batch.size() > 0) {
        hBaseTable.batch(batch)
        batch.clear()
      }
      hBaseTable.close()
      con.close()
      hbaseConf.clear()
    }
    log.error(getlabeltime+">>>>>>>>>>>>>>>>>>>>>>>>>写入完毕")
  }









  def savenetflowresult(endDF: DataFrame,spark:SparkSession,zokMaster:String,zokPort:String,postgresurl:String,user:String,password:String) ={
    import spark.implicits._
    log.error(">>>>>>>>>>>>>>>>>>>>>>>>>>>>流量分析结果<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    endDF.show(10,false)
    //入库之前的转换
    val endDS=endDF.map{
      case org.apache.spark.sql.Row(attacktype:String,srcip:String,dstip:String,
      proto:String,srcport:String,dstport:String,recordtime:String,flownums:String,flownuma:String)=>
        ENDNET(
          srcip,dstip,attacktype,Timestamp.valueOf(recordtime),
          proto,srcport.toDouble.toInt,dstport.toDouble.toInt,flownums.toDouble.toInt,flownuma.toDouble.toInt
        )
    }
    endDS.show(10,false)
    endDS.distinct.foreachPartition{
      PartLine => {
        //插入到hbase
        //插入到sql
        val log=Logger.getLogger(this.getClass)
        val db=new DBUtils
        val hbconn=db.getPhoniexConnect(zokMaster+":"+zokPort)
        val sqlconn=db.getSQLConnect(sqlurl=postgresurl,username=user,password=password)
        hbconn.setAutoCommit(false);sqlconn.setAutoCommit(false)
        //        ATTACKTYPE      SRCIP   DSTIP   PROTO   SRCPORT DSTPORT RECORDTIME      FLOWNUMA
        val prephb  = hbconn.prepareStatement("UPSERT INTO t_siem_netflow_rfpredict (\"ROW\",\"SRCIP\",\"DSTIP\",\"ATTACKTYPE\",\"RECORDTIME\",\"PROTO\",\"SRCPORT\",\"DSTPORT\",\"FLOWNUMS\",\"FLOWNUMA\") VALUES (?,?,?,?,?,?,?,?,?,?)")
        val prepsql = sqlconn.prepareStatement("INSERT INTO t_siem_netflow_rfpredict (RECORDID,SRCIP,DSTIP,ATTACKTYPE,RECORDTIME,PROTO,SRCPORT,DSTPORT,FLOWNUMS,FLOWNUMA) VALUES (?,?,?,?,?,?,?,?,?,?)")
        PartLine.foreach{
          line =>
            try{
              val row=ROWUtils.genaralROW()
              prephb.setString(1,row);prepsql.setString(1,row)
              prephb.setString(2,line.SRCIP);prepsql.setString(2,line.SRCIP)
              prephb.setString(3,line.DSTIP);prepsql.setString(3,line.DSTIP)
              prephb.setString(4,line.ATTACKTYPE);prepsql.setString(4,line.ATTACKTYPE)
              prephb.setTimestamp(5,line.RECORDTIME);prepsql.setTimestamp(5,line.RECORDTIME)
              prephb.setString(6,line.PROTO);prepsql.setString(6,line.PROTO)
              prephb.setInt(7,line.SRCPORT);prepsql.setInt(7,line.SRCPORT)
              prephb.setInt(8,line.DSTPORT);prepsql.setInt(8,line.DSTPORT)
              prephb.setInt(9,line.FLOWNUMS);prepsql.setInt(9,line.FLOWNUMS)
              prephb.setInt(10,line.FLOWNUMA);prepsql.setInt(10,line.FLOWNUMA)
              prephb.executeUpdate;prepsql.executeUpdate
            }catch {
              case e: Exception => {
                log.error(getlabeltime+s"!!!!!!插入失败!!!!!!!!!!" + e.getMessage)
              }
            }
        }
        hbconn.commit;sqlconn.commit
        hbconn.close;sqlconn.close
      }
    }
  }

  def savemoniflowresult(resDF: DataFrame,spark:SparkSession,postgresurl:String,user:String,password:String) ={
    import spark.implicits._
    log.error(">>>>>>>>>>>>>>>>>>>>>>>>>>>>网络审计分析结果<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    resDF.show(10,false)
    //入库之前的转换
    val endDS=resDF.map{
          case org.apache.spark.sql.Row(id:String,date:String,srcip:String,dstip:String,uri:String,url:String,predictionnb:String,probabilitynb:String,predictionlr:String,probabilitylr:String)=>
            ENDMONI(id:String,Timestamp.valueOf(date),srcip:String,dstip:String,uri:String,url:String,predictionnb.toDouble,probabilitynb.toDouble,predictionlr.toDouble,probabilitylr.toDouble)
        }
    endDS.show(10,false)
    endDS.distinct.foreachPartition{
      PartLine => {
        //插入到hbase
        //插入到sql
        val log=Logger.getLogger(this.getClass)
        val db=new DBUtils
        val sqlconn=db.getSQLConnect(sqlurl=postgresurl,username=user,password=password)
        sqlconn.setAutoCommit(false)
        val prepsql = sqlconn.prepareStatement("INSERT INTO T_MONI_FLOW_PREDICT (id,date,srcip,dstip,uri,url,predictionnb,probabilitynb,predictionlr,probabilitylr) VALUES (?,?,?,?,?,?,?,?,?,?)")
        PartLine.foreach{
          line =>
            try{
              prepsql.setString(1,line.id)
              prepsql.setTimestamp(2,line.date)
              prepsql.setString(3,line.srcip)
              prepsql.setString(4,line.dstip)
              prepsql.setString(5,line.uri)
              prepsql.setString(6,line.url)
              prepsql.setDouble(7,line.predictionnb)
              prepsql.setDouble(8,line.probabilitynb)
              prepsql.setDouble(9,line.predictionlr)
              prepsql.setDouble(10,line.probabilitylr)
              prepsql.executeUpdate
            }catch {
              case e: Exception => {
                log.error(getlabeltime+s"!!!!!!插入失败!!!!!!!!!!" + e.getMessage)
              }
            }
        }
        sqlconn.commit
        sqlconn.close
      }
    }
  }

//  def savemoniflowresult(resDF:DataFrame,spark:SparkSession,postgresurl:String,user:String,password:String) ={
//    import spark.implicits._
//    log.error(">>>>>>>>>>>>>>>>>>>>>>>>>>>>网络审计分析结果<<<<<<<<<<<<<<<<<<<<<<<<<<<")
//    resDF.show(10,false)
//    val postgprop=new Properties()
//    postgprop.put("user", user)
//    postgprop.put("password", password)
//    //入库之前的转换
//    val endDS=resDF.map{
//      case org.apache.spark.sql.Row(id:String,date:String,srcip:String,dstip:String,uri:String,predictionnb:String,probabilitynb:String,predictionlr:String,probabilitylr:String)=>
//        ENDMONI(id:String,Timestamp.valueOf(date),srcip:String,dstip:String,uri:String,predictionnb.toDouble,probabilitynb.toDouble,predictionlr.toDouble,probabilitylr.toDouble)
//    }
//    endDS.write.mode(SaveMode.Append).jdbc(postgresurl, "T_MONI_FLOW_predict",postgprop)
//  }












  def PtoString(Pdata:Any)={
    if(Pdata==null) "-" else Pdata.toString
  }
  def getNETFLOWRows(spark: SparkSession,zokhost:String,zokport:String,scan: Scan=new Scan) = {
    import spark.implicits._
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum",zokhost)
    conf.set("hbase.zookeeper.property.clientPort",zokport)
    conf.set(TableInputFormat.INPUT_TABLE,"T_SIEM_NETFLOW")
    conf.set(TableInputFormat.SCAN,convertScanToString(scan))
    val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    val getDS = hbaseRDD.map{
      r=>
        val util= getStringValue(r,"NETFLOW")
        val STARTTIME=util(ColConfig("STARTTIME",93))
        val ENDTIME=util(ColConfig("ENDTIME",93))
        val PROTO=util(ColConfig("PROTO",12))
        val PACKERNUM=util(ColConfig("PACKERNUM",4))
        val BYTESIZE=util(ColConfig("BYTESIZE",4))
        val SRCIP=util(ColConfig("SRCIP",12))
        val DSTIP=util(ColConfig("DSTIP",12))
        val RECORDTIME=util(ColConfig("RECORDTIME",93))
        val ROW=util(ColConfig("ROW",12))
        val SRCPORT=util(ColConfig("SRCPORT",4))
        val DSTPORT=util(ColConfig("DSTPORT",4))
        var cols=NETFLOWHData(STARTTIME = STARTTIME,ENDTIME=ENDTIME,PROTO=PROTO,PACKERNUM=PACKERNUM,BYTESIZE=BYTESIZE,SRCIP=SRCIP,DSTIP=DSTIP,RECORDTIME=RECORDTIME,ROW=ROW,SRCPORT=SRCPORT,DSTPORT=DSTPORT)
        cols.copy(FLAG = cols.getflag)
    }.toDS
    getDS
  }
  def getMONIFLOWRows(spark: SparkSession,zokhost:String,zokport:String,scan: Scan=new Scan) = {
    import spark.implicits._
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum",zokhost)
    conf.set("hbase.zookeeper.property.clientPort",zokport)
    conf.set(TableInputFormat.INPUT_TABLE,"T_MONI_FLOW")
    conf.set(TableInputFormat.SCAN,convertScanToString(scan))
    val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    val getDS = hbaseRDD.map { r =>
      val util= getStringValue(r,"FLOW")
      val ROW = util(ColConfig("ROW",12))
      val DATE = util(ColConfig("DATE",93))
      val SRCIP = util(ColConfig("SRCIP",12))
      val DSTIP = util(ColConfig("DSTIP",12))
      val APPPROTO = util(ColConfig("APPPROTO",12))
      val METHOD = util(ColConfig("METHOD",12))
      val SRCPORT = util(ColConfig("SRCPORT",4))
      val DSTPORT = util(ColConfig("DSTPORT",4))
      val URI = util(ColConfig("URI",12))
      val HOST = util(ColConfig("HOST",12))
      var cols=MONIFLOWHData(ROW=ROW,DATE=DATE,SRCIP=SRCIP,DSTIP=DSTIP,APPPROTO=APPPROTO,METHOD=METHOD,SRCPORT=SRCPORT,DSTPORT=DSTPORT,URI=URI,HOST=HOST)
      cols.copy(FLAG = cols.getflag)
    }.toDS
    getDS
  }
 def GENERALLOGs(spark: SparkSession,zokhost:String,zokport:String,scan: Scan=new Scan) = {
   val conf = HBaseConfiguration.create()
   conf.set("hbase.zookeeper.quorum",zokhost)
   conf.set("hbase.zookeeper.property.clientPort",zokport)
   conf.set(TableInputFormat.INPUT_TABLE,"T_SIEM_GENERAL_LOG")
   conf.set(TableInputFormat.SCAN,convertScanToString(scan))
    import spark.implicits._
    val sc=spark.sparkContext
    val hbaseRDD = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    val getDS = hbaseRDD.map { r =>
      val util= getStringValue(r,"GLOG")
      val RECORDID = util(ColConfig("RECORDID",12))
      val EVENTDETAIL = util(ColConfig("EVENTDETAIL",12))
      val SOURCEIP = util(ColConfig("SOURCEIP",12))
      val DESTIP = util(ColConfig("DESTIP",12))
      val FIRSTRECVTIME = util(ColConfig("FIRSTRECVTIME",93))
      var cols=GLOGHData(RECORDID=RECORDID,EVENTDETAIL=EVENTDETAIL,SOURCEIP=SOURCEIP,DESTIP=DESTIP,FIRSTRECVTIME=FIRSTRECVTIME)
      cols.copy(FLAG = cols.getflag)
    }.toDS
   getDS
  }
  def EVENTRESULT(spark:SparkSession,zokhost:String,zokport:String,scan: Scan=new Scan) = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum",zokhost)
    conf.set("hbase.zookeeper.property.clientPort",zokport)
    conf.set(TableInputFormat.INPUT_TABLE,"T_SIEM_EVENT_RESULT")
    conf.set(TableInputFormat.SCAN,convertScanToString(scan))
    import spark.implicits._
    val sc=spark.sparkContext
    val hbaseRDD = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    val getDS = hbaseRDD.map { r =>
      val util= getStringValue(r,"EVENT")
      val ROW = util(ColConfig("ROW",12))
      val LOGID = util(ColConfig("LOGID",12))
      val EVENT_RULE_ID = util(ColConfig("EVENT_RULE_ID",12))
      val EVENT_RULE_NAME = util(ColConfig("EVENT_RULE_NAME",12))
      val SOURCEIP = util(ColConfig("SOURCEIP",12))
      val DESTIP = util(ColConfig("DESTIP",12))
      val OPENTIME = util(ColConfig("OPENTIME",93))
      var cols=EVENTRESHData(ROW=ROW,LOGID=LOGID,EVENT_RULE_ID=EVENT_RULE_ID,EVENT_RULE_NAME=EVENT_RULE_NAME,SOURCEIP=SOURCEIP,DESTIP=DESTIP,OPENTIME=OPENTIME)
      cols.copy(FLAG = cols.getflag)
    }.toDS
    getDS
  }
  def EVENTRULE(spark:SparkSession,zokhost:String,zokport:String) = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum",zokhost)
    conf.set("hbase.zookeeper.property.clientPort",zokport)
    conf.set(TableInputFormat.INPUT_TABLE,"T_SIEM_EVENT_RULE_DEF")
    import spark.implicits._
    val sc=spark.sparkContext
    val hbaseRDD = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    val getDS = hbaseRDD.map{
      r=>
        val util= getStringValue(r,"0")
        val EVENT_RULE_ID=util(ColConfig("EVENT_RULE_ID",12))
        val EVENT_SUB_TYPE=util(ColConfig("EVENT_SUB_TYPE",12))
        var cols=EVENTRULEHData(EVENT_RULE_ID=EVENT_RULE_ID,EVENT_SUB_TYPE=EVENT_SUB_TYPE)
        cols.copy(FLAG = cols.getflag)
    }.toDS
    getDS
  }
  def EVENTTYPE(spark:SparkSession,zokhost:String,zokport:String) = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum",zokhost)
    conf.set("hbase.zookeeper.property.clientPort",zokport)
    conf.set(TableInputFormat.INPUT_TABLE,"T_SIEM_EVENT_TYPE_BASE")
    import spark.implicits._
    val sc=spark.sparkContext
    val hbaseRDD = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    val getDS = hbaseRDD.map{
      r=>
        val util= getStringValue(r,"0")
        val EVENT_TYPE_ID=util(ColConfig("EVENT_TYPE_ID",12))
        val EVENT_TYPE_NAME=util(ColConfig("EVENT_TYPE_NAME",12))
        var cols=EVENTTYPEHData(EVENT_TYPE_ID=EVENT_TYPE_ID,EVENT_TYPE_NAME=EVENT_TYPE_NAME)
        cols.copy(FLAG = cols.getflag)
    }.toDS
    getDS
  }


  def getTESTRows(spark:SparkSession,zokhost:String,zokport:String,scan: Scan=new Scan) = {
    import spark.implicits._
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum",zokhost)
    conf.set("hbase.zookeeper.property.clientPort",zokport)
    conf.set(TableInputFormat.INPUT_TABLE,"T_TESTROW")
    conf.set(TableInputFormat.SCAN,convertScanToString(scan))
    val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    val getRDD = hbaseRDD.map{
      r=>
        val util= getStringValue(r,"T")
        val COSTTIME=util(ColConfig(colname="COSTTIME",coltype=12))
        val TCP=util(ColConfig("TCP",12))
        val UDP=util(ColConfig("UDP",12))
        val ICMP=util(ColConfig("ICMP",12))
        val GRE=util(ColConfig("GRE",12))
        val PROTO=util(ColConfig("PROTO",12))
        val PACKERNUM=util(ColConfig("PACKERNUM",12))
        val BYTESIZE=util(ColConfig("BYTESIZE",12))
        val SRCIP=util(ColConfig("SRCIP",12))
        val DSTIP=util(ColConfig("DSTIP",12))
        val RECORDTIME=util(ColConfig("RECORDTIME",12))
        val ROW=util(ColConfig("ROW",12))
        val SRCPORT=util(ColConfig("SRCPORT",12))
        val DSTPORT=util(ColConfig("DSTPORT",12))
        var cols=TESTROW(COSTTIME = COSTTIME,TCP=TCP,UDP=UDP,ICMP=ICMP,GRE=GRE,PROTO=PROTO,PACKERNUM=PACKERNUM,BYTESIZE=BYTESIZE,SRCIP=SRCIP,DSTIP=DSTIP,RECORDTIME=RECORDTIME,ROW=ROW,SRCPORT=SRCPORT,DSTPORT=DSTPORT)
        cols.copy(FLAG = cols.getflag)
    }.toDS
    getRDD
  }

}









//def saveAssetsresult(resDF:DataFrame,zokhost:String,zokport:String,putTableName:String,colfamily:String)={
//  //获取字段名
//  log.error("资产画像")
//  resDF.show(5,false)
//  val colnames=resDF.columns
//  log.error(">>>>>>>>>>>>>>>>>>>>字段名为"+colnames.toList)
//  //    val bsize=1000
//  resDF.foreachPartition(
//  PartLine => {
//  log.error(">>>>>>>>>>>>>>>>>>>>>>>>>>开始写入数据<<<<<<<<<<<<<<<<<<<<<<<")
//  log.error(">>>>>>>>>>>>>>>>>>>>>>主机"+zokhost)
//  log.error(">>>>>>>>>>>>>>>>>>>>>>端口"+zokport)
//  log.error(">>>>>>>>>>>>>>>>>>>>>>表名"+putTableName)
//  val hbaseConf = HBaseConfiguration.create()
//  hbaseConf.set("hbase.zookeeper.quorum",zokhost)
//  hbaseConf.set("hbase.zookeeper.property.clientPort",zokport)
//  hbaseConf.set(HConstants.HBASE_RPC_TIMEOUT_KEY, "2000000")
//  val con=ConnectionFactory.createConnection(hbaseConf)
//  val hBaseTable = con.getTable(TableName.valueOf(putTableName))
//  //        var batch = new java.util.ArrayList[org.apache.hadoop.hbase.client.Row]()
//  PartLine.foreach(
//  line => {
//  log.error(">>>>>>>>>>>>>>>>>>进入某一行>>>>>>>>>>>>>>>>>>>>>>>>")
//  val log = Logger.getLogger(this.getClass)
//  var put = new Put(Bytes.toBytes(ROWUtils.genaralROW()))
//  val util=saveToHBase(put,colfamily,line)
//  colnames.foreach{
//  col=>
//  //                try {
//  if(col != "ROW") {
//  util(ColConfig(colname = col, coltype = getconf(putTableName + "." + col).toInt))
//  //                  batch.add(util(ColConfig(colname = col, coltype = getconf(putTableName + "." + col).toInt)))
//}
//  //                }catch {
//  //                  case e:Exception=>
//  //                    log.error(s"!!!!!!!!!!字段错误!!!!!!!!!!")
//  //                    log.error(s">>>>>>>>>>>>>>>字段名："+col)
//  //                    log.error(s"!!!!!!!!!!字段错误!!!!!!!!!!")
//  //                    log.error(s">>>>>>>>>>>>>>>字段名："+col)
//  //                }
//  //            try{
//  //            if(batch.size%bsize==0){hBaseTable.batch(batch);batch.clear()}
//  //            }
//  //            catch {
//  //              case e:Exception=>
//  //                log.error(s"!!!!!!!!!!批量插入失败!!!!!!!!!!")
//  //                log.error(s"!!!!!!!!!!批量插入失败!!!!!!!!!!")
//  //                e.printStackTrace()
//  //            }
//}
//  hBaseTable.put(put)
//}
//  )
//  //        try{
//  //        if(batch.size>0){
//  //        hBaseTable.batch(batch)
//  //        batch.clear()
//  //        }
//  //        }
//  //        catch {
//  //          case e:Exception=>
//  //            log.error(s"!!!!!!!!!!最后一部分插入失败!!!!!!!!!!")
//  //            e.printStackTrace()
//  //        }
//  hBaseTable.close()
//  con.close()
//  hbaseConf.clear()
//}
//  )
//  log.error("写入完毕")
//}















//def saveCENTER(resDF:DataFrame,postgprop:Properties) ={
//  log.error("中心点")
//  resDF.show(10)
//  val bsize=5000
//  val putTableName = "T_SIEM_KMEANS_CENTER"
//  resDF.foreachPartition(
//  PartLine => {
//  val hbaseConf = HBaseConfiguration.create()
//  hbaseConf.set("hbase.zookeeper.quorum",postgprop.getProperty("zookeeper.host"))
//  hbaseConf.set("hbase.zookeeper.property.clientPort",postgprop.getProperty("zookeeper.port"))
//  hbaseConf.set(HConstants.HBASE_RPC_TIMEOUT_KEY, "2000000")
//  val con=ConnectionFactory.createConnection(hbaseConf)
//  val hBaseTable = con.getTable(TableName.valueOf(putTableName))
//  var batch = new java.util.ArrayList[org.apache.hadoop.hbase.client.Row]()
//  PartLine.foreach(
//  line => {
//  val put = new Put(Bytes.toBytes(ROWUtils.genaralROW()))
//  val util=saveToHBase(put,"CENTER",line)
//  try {
//  util(ColConfig("NAME", 12))
//}
//  util(ColConfig("CENTER_INDEX",12))
//  util(ColConfig("CENTER_INFO",12))
//  util(ColConfig("CLNO",12))
//  util(ColConfig("CLNO_P", 12))
//  util(ColConfig("CENTER_INDEX_P", 12))
//  util(ColConfig("TYPE",4))
//  util(ColConfig("CLTIME",93))
//  util(ColConfig("DEMO",12))
//  util(ColConfig("DATA_ORIGIN",12))
//  batch.add(put)
//  if(batch.size%bsize==0){hBaseTable.batch(batch);batch.clear()}
//}
//
//  )
//  if(batch.size>0){hBaseTable.batch(batch);batch.clear()}
//  hBaseTable.close()
//  con.close()
//  hbaseConf.clear()
//}
//  )
//  log.error("写入完毕")
//}
//  def saveCLUSTER(resDF:DataFrame,postgprop:Properties) ={
//  val bsize=5000
//  val putTableName = "T_SIEM_KMEANS_CLUSTER"
//  resDF.foreachPartition(
//  PartLine => {
//  val hbaseConf = HBaseConfiguration.create()
//  hbaseConf.set("hbase.zookeeper.quorum",postgprop.getProperty("zookeeper.host"))
//  hbaseConf.set("hbase.zookeeper.property.clientPort",postgprop.getProperty("zookeeper.port"))
//  hbaseConf.set(HConstants.HBASE_RPC_TIMEOUT_KEY, "2000000")
//  val con=ConnectionFactory.createConnection(hbaseConf)
//  val hBaseTable = con.getTable(TableName.valueOf(putTableName))
//  var batch = new java.util.ArrayList[org.apache.hadoop.hbase.client.Row]()
//  PartLine.foreach(
//  line => {
//  val put = new Put(Bytes.toBytes(ROWUtils.genaralROW()))
//  val util=saveToHBase(put,"CLUSTER",line)
//  util(ColConfig("NAME",12))
//  util(ColConfig("CENTER_INDEX",12))
//  util(ColConfig("CLNO",12))
//  util(ColConfig("DISTANCE",8))
//  util(ColConfig("CLUSTER_INFO",12))
//  util(ColConfig("CLTIME",12))
//  util(ColConfig("DEMO",12))
//  util(ColConfig("DATA_ORIGIN",12))
//  batch.add(put)
//  if(batch.size%bsize==0){hBaseTable.batch(batch);batch.clear()}
//}
//
//  )
//  if(batch.size>0){hBaseTable.batch(batch);batch.clear()}
//  hBaseTable.close()
//  con.close()
//  hbaseConf.clear()
//}
//  )
//  log.error("写入完毕")
//}


































//    val bsize=5000
//    val putTableName = "T_SIEM_NETFLOW_RFPREDICT"
//    readCSV.foreachPartition(
//      PartLine => {
//        val hbaseConf = HBaseConfiguration.create()
//        hbaseConf.set("hbase.zookeeper.quorum","hadoop30")
//        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
//        hbaseConf.set(HConstants.HBASE_RPC_TIMEOUT_KEY, "2000000")
//        val con=ConnectionFactory.createConnection(hbaseConf)
//        val hBaseTable = con.getTable(TableName.valueOf(putTableName))
//        var batch = new util.ArrayList[Row]()
//        PartLine.foreach(
//          line => {
//            val put = new Put(Bytes.toBytes(ROWUtils.genaralROW()))
//            val util=saveToHBase(put,"T",line)
//            util(ColConfig("RECORDID",12))
//            util(ColConfig("SRCIP",12))
//            util(ColConfig("DSTIP",12))
//            util(ColConfig("ATTACKTYPE",12))
//            util(ColConfig("RECORDTIME",12))
//            util(ColConfig("COSTTIME",12))
//            util(ColConfig("PROTO",12))
//            util(ColConfig("PACKERNUM",12))
//            util(ColConfig("BYTESIZE",12))
//            util(ColConfig("FLOWDIRECTION",12))
//            util(ColConfig("SRCPORT",12))
//            util(ColConfig("DSTPORT",12))
//            util(ColConfig("FLOWNUMA",12))
//            batch.add(put)
//            if(batch.size%bsize==0){hBaseTable.batch(batch);batch.clear()}
//          }
//
//        )
//        if(batch.size>0){hBaseTable.batch(batch);batch.clear()}
//        hBaseTable.close()
//        con.close()
//        hbaseConf.clear()
//      }
//    )
//
//
//
//    log.error("写入完毕")




















//def testgetNETFLOWRows(sqlContext:SQLContext,sc:SparkContext,conf:Configuration,m:Int,d:Int) = {
//  import sqlContext.implicits._
//  val hbaseRDD = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
//  .map {r=>
//  val time=new Date(Bytes.toString(r._2.getRow.drop(6)).toLong*1000)
//  log.error((time.getMonth+1,time.getDate))
//  (r,time.getMonth+1,time.getDate)
//}.filter(x=>x._2==m&&x._3==d)
//  val getRDD = hbaseRDD.map { r =>
//  val col1=r._1._2.getValue(Bytes.toBytes("NETFLOW"),Bytes.toBytes("STARTTIME"))
//  val col2=r._1._2.getValue(Bytes.toBytes("NETFLOW"),Bytes.toBytes("ENDTIME"))
//  val col3=r._1._2.getValue(Bytes.toBytes("NETFLOW"),Bytes.toBytes("PROTO"))
//  val col4=r._1._2.getValue(Bytes.toBytes("NETFLOW"),Bytes.toBytes("PACKERNUM"))
//  val col5=r._1._2.getValue(Bytes.toBytes("NETFLOW"),Bytes.toBytes("BYTESIZE"))
//  val col6=r._1._2.getValue(Bytes.toBytes("NETFLOW"),Bytes.toBytes("SRCIP"))
//  val col7=r._1._2.getValue(Bytes.toBytes("NETFLOW"),Bytes.toBytes("DSTIP"))
//  val col8=r._1._2.getValue(Bytes.toBytes("NETFLOW"),Bytes.toBytes("RECORDTIME"))
//  val col9=r._1._2.getRow
//  val col10=r._1._2.getValue(Bytes.toBytes("NETFLOW"),Bytes.toBytes("SRCPORT"))
//  val col11=r._1._2.getValue(Bytes.toBytes("NETFLOW"),Bytes.toBytes("DSTPORT"))
//
//  val flag=if(col1==null||col2==null||col3==null||col4==null||col4==null||col6==null||col7==null||col8==null||col9==null||col10==null||col11==null) 0 else 1
//  (col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,flag)
//}.filter(_._12>0).map(line=>
//  (
//  PTimestamp.INSTANCE.toObject(line._1).toString, //stime
//  PTimestamp.INSTANCE.toObject(line._2).toString, //etime
//  Bytes.toString(line._3),                        //proto
//  PInteger.INSTANCE.toObject(line._4).toString,   //pnum
//  PInteger.INSTANCE.toObject(line._5).toString,   //bsize
//  Bytes.toString(line._6),                        //sip
//  Bytes.toString(line._7),                        //dip
//  PTimestamp.INSTANCE.toObject(line._8).toString, //rtime
//  Bytes.toString(line._9),                        //row
//  PInteger.INSTANCE.toObject(line._10).toString, //sport
//  PInteger.INSTANCE.toObject(line._11).toString //dport
//  )
//  ).toDS("STARTTIME","ENDTIME","PROTO","PACKERNUM","BYTESIZE","SRCIP","DSTIP","RECORDTIME","ROW","SRCPORT","DSTPORT")
//  getRDD
//}
//
//def isExist(tableName: String,conf:Configuration) {
//  val hAdmin: HBaseAdmin = new HBaseAdmin(conf)
//  hAdmin.tableExists(tableName)
//}
//
//  def createTable(tableName: String, columnFamilys: Array[String],conf:Configuration): Unit = {
//  val hAdmin: HBaseAdmin = new HBaseAdmin(conf)
//  if (hAdmin.tableExists(tableName)) {
//  log.error("表" + tableName + "已经存在")
//  return
//} else {
//  val tableDesc: HTableDescriptor = new HTableDescriptor(tableName)
//  for (columnFaily <- columnFamilys) {
//  tableDesc.adDSamily(new HColumnDescriptor(columnFaily))
//}
//  hAdmin.createTable(tableDesc)
//  log.error("创建表成功")
//}
//}
//
//  def deleteTable(tableName: String,conf:Configuration): Unit = {
//  val admin: HBaseAdmin = new HBaseAdmin(conf)
//  if (admin.tableExists(tableName)) {
//  admin.disableTable(tableName)
//  admin.deleteTable(tableName)
//  log.error("删除表成功!")
//} else {
//  log.error("表" + tableName + " 不存在")
//}
//}
//
//  def addRows(conf:Configuration,tableName: String, row: Array[String], columnFaily: String, column: Array[String], value: Array[Array[String]]): Unit = {
//  val table: HTable = new HTable(conf, tableName)
//  for (i <- 0 to value.length - 1) {
//  if (row(0) == "*") {
//  val put: Put = new Put(Bytes.toBytes(UUID.randomUUID().toString))
//  for (j <- 0 to column.length - 1) {
//  put.add(Bytes.toBytes(columnFaily), Bytes.toBytes(column(j)), Bytes.toBytes(value(i)(j)))
//  table.put(put)
//}
//}
//  else {
//  val put: Put = new Put(Bytes.toBytes(row(i)))
//  for (j <- 0 to column.length - 1) {
//  put.add(Bytes.toBytes(columnFaily), Bytes.toBytes(column(j)), Bytes.toBytes(value(i)(j)))
//  table.put(put)
//}
//}
//
//}
//}
//
//  def addRow(conf:Configuration,tableName: String, row: String, columnFaily: String, column: String, value: String): Unit = {
//  val table: HTable = new HTable(conf, tableName)
//  val put: Put = new Put(Bytes.toBytes(row))
//  put.add(Bytes.toBytes(columnFaily), Bytes.toBytes(column), Bytes.toBytes(value))
//  table.put(put)
//}
//
//  def delRow(conf:Configuration,tableName: String, row: String): Unit = {
//  val table: HTable = new HTable(conf, tableName)
//  val delete: Delete = new Delete(Bytes.toBytes(row))
//  table.delete(delete)
//}
//
//  def delMultiRows(conf:Configuration,tableName: String, rows: Array[String]): Unit = {
//  val table: HTable = new HTable(conf, tableName)
//  val deleteList = for (row <- rows) yield new Delete(Bytes.toBytes(row))
//  table.delete(deleteList.toSeq.asJava)
//}
//
//  def getRow(conf:Configuration,tableName: String, row: String): Unit = {
//  val table: HTable = new HTable(conf, tableName)
//  val get: Get = new Get(Bytes.toBytes(row))
//  val result: Result = table.get(get)
//  for (rowKv <- result.raw()) {
//  log.error(new String(rowKv.getFamily))
//  log.error(new String(rowKv.getQualifier))
//  log.error(rowKv.getTimestamp)
//  log.error(new String(rowKv.getRow))
//  log.error(new String(rowKv.getValue))
//}
//}
//
//  def getAllRows(tableName: String,conf:Configuration): Unit = {
//  val table: HTable = new HTable(conf, tableName)
//  val results: ResultScanner = table.getScanner(new Scan())
//  val it: util.Iterator[Result] = results.iterator()
//  while (it.hasNext) {
//  val next: Result = it.next()
//  for(kv <- next.raw()){
//  log.error(new String(kv.getRow))
//  log.error(new String(kv.getFamily))
//  log.error(new String(kv.getQualifier))
//  log.error(new String(kv.getValue))
//  log.error(kv.getTimestamp)
//  log.error("---------------------")
//}
//}
//}
