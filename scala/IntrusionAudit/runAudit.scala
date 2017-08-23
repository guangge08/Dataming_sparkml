package IntrusionAudit
import java.io.File
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.elasticsearch.spark.sql._
import utils.{ConfSupport, HbaseAPI, LogSupport}

import scala.util.Try

/**
  * Created by duan on 2017/4/6.
  */
object runAudit extends LogSupport with ConfSupport{
  val directory = new File("..")
  val filePath = directory.getAbsolutePath //设定为上级文件夹 获取绝对路径
  PropertyConfigurator.configure(filePath + "/conf/log4j.properties")

  def main(args: Array[String]) {
    log.error("<<<<<<<<<<<<<<<<<<<<<<<入侵审计任务开始于" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + ">>>>>>>>>>>>>>>>>>>>>>")
    println(">>>>>>>>>>>>>>>>>选取运行模式  day表示仅仅取一天的数据 其他的表示取全部数据")
    log.error(">>>>>>>>>>>>>>>>>选取的运行模式为"+args(0))

    //读取配置文件
    loadconf("/models.conf")
    val sparkconf = new SparkConf()
      .setMaster(getconf("IntrusionAudit.master"))
      .setAppName(getconf("IntrusionAudit.appname"))
      .set("spark.port.maxRetries", getconf("IntrusionAudit.portmaxRetries"))
      .set("spark.cores.max", getconf("IntrusionAudit.coresmax"))
      .set("spark.executor.memory", getconf("IntrusionAudit.executormemory"))
      .set("es.nodes",getconf("IntrusionAudit.esnodes"))
      .set("es.port",getconf("IntrusionAudit.esport"))
    val sparkSession = SparkSession.builder().config(sparkconf).getOrCreate()
    val glogIndex=getconf("IntrusionAudit.glogIndex")
    val glogAuditIndex=getconf("IntrusionAudit.glogAuditIndex")
    val time1=System.currentTimeMillis()
    var audit=new Audit(sparkSession,args,glogIndex,glogAuditIndex)
    audit.getsqlEvent(args(0))
    val time2=System.currentTimeMillis()
    log.error("<<<<<<<<<<<<<<<<<<<<<<<入侵审计任务结束于" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + ">>>>>>>>>>>>>>>>>>>>>>")
    log.error("<<<<<<<<<<<<<<<<<<<<<<<入侵审计任务耗时"+(time2-time1)/1000+"秒>>>>>>>>>>>>>>>>>>>>>>")
  }
}
class Audit(spark:SparkSession,args: Array[String],glogIndex:String,glogAuditIndex: String) extends Serializable with HbaseAPI{
  def getsqlEvent(mode:String)={
    //  设置时间范围
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, -1)
    val time = cal.getTime
    val yestoday = Try(args(1).trim).getOrElse(new SimpleDateFormat("yyyy-MM-dd").format(time))
    val today = Try(args(2).trim).getOrElse(new SimpleDateFormat("yyyy-MM-dd").format(new Date))
    log.error(yestoday+"|||"+today)
    val startTime = Timestamp.valueOf(yestoday + " 00:00:00").getTime
    val endTime = Timestamp.valueOf(today + " 00:00:00").getTime
    val query="{\"query\":{\"range\":{\"firstrecvtime\":{\"gte\":"+startTime+",\"lte\":"+endTime+"}}}}"


//    选取程序运行模式
    val glogDF=if(mode=="day")spark.esDF(glogIndex,query) else spark.esDF(glogIndex)
    //范式化日志表 其中的sql注入记录
    val sqlDF=glogDF.where("eventname='HTTP_SQL注入攻击' OR eventname LIKE 'SQL Injection%'")
    sqlDF.createOrReplaceTempView("sqltmp");glogDF.createOrReplaceTempView("glogtmp")
    //如果存在sql注入攻击
    if(sqlDF.take(10).length>0){
      val auditDF=spark.sql(
        """
          |SELECT sqltmp.*,glogtmp.orglog as alllog,sqltmp.firstrecvtime as eventtime,glogtmp.firstrecvtime as logtime FROM sqltmp
          |LEFT JOIN glogtmp ON (sqltmp.row<>glogtmp.row AND sqltmp.sourceip=glogtmp.sourceip AND sqltmp.destip=glogtmp.destip)
        """.stripMargin)
      println(">>>>>>>>>>>>>>>等待审计>>>>>>>>>>>>")
      auditDF.show(3,false)
      val auditUDF=org.apache.spark.sql.functions.udf(
        (eventDetail:String,eventTime:String,logTime:String)=>{
          var reach="0"
          if(eventTime!=null&&logTime!=null){
            val timelength=eventTime.toLong-logTime.toLong
            if(eventDetail.contains("sql")&&timelength<1000&&timelength>(-1000)){reach="1"}
          }
          reach
        }
      )
      val resDF=auditDF.withColumn("reach",auditUDF(auditDF("alllog"),auditDF("eventtime"),auditDF("logtime")))
      insertresult(resDF.drop("alllog","eventtime","logtime").distinct)
    } else {
      log.error("!!!!!!!!!!!!!无sql攻击事件!!!!!!!!!!!!!!")
    }
  }
  def insertresult(resDF: Dataset[Row])={
    println(">>>>>>>>>>待入库的最终结果>>>>>>>>>>>")
    resDF.show(5,false)
    println(">>>>>>>>总共条数为"+resDF.count)
    println(">>>>>>>>其中可达条数为"+resDF.filter("reach=1").count)
    resDF.saveToEs(glogAuditIndex)
  }
}
