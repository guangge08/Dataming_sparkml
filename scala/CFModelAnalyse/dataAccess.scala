package CFModelAnalyse


import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.elasticsearch.spark.sql._




/**
  * Created by Administrator on 2017/7/24.
  */
trait dataAccess {

  //连接es获取数据
  def getSIEMDataFromEs(spark: SparkSession, dataOrigin:String, timecolumnname:String,properties: Properties,useProperties:Boolean = false) = {
    println("开始连接es获取数据")
    //es表名
    val tableName = properties.getProperty(dataOrigin)//"es.netflow.table.name"
    //判断是否使用配置文件上的时间
    var start = ""
    var end = ""
    if(useProperties){
      start = properties.getProperty("read.es.start.time")
      end = properties.getProperty("read.es.end.time")
    }
    else{
      start = getTime(properties.getProperty("read.es.start.time.int").toInt)
      end = getTime(0)
    }
    //抓取时间范围
    val starttime = Timestamp.valueOf(start).getTime
    val endtime = Timestamp.valueOf(end).getTime
    val query = "{\"query\":{\"range\":{\""+timecolumnname+"\":{\"gte\":" + starttime + ",\"lte\":" + endtime + "}}}}"
    //抓取数据下来
    val rowDF = spark.esDF(tableName, query)
    rowDF.show(10,false)
    rowDF
  }

  def saveToES(data:DataFrame, dataOrigin:String, properties: Properties, sql: SQLContext)= {
    val esoption = properties.getProperty(dataOrigin)
    try {
      data.saveToEs(esoption)
      println("complete:save to ES !")
    } catch {
      case e: Exception => println("error on save to ES：" + e.getMessage)
    }
    finally {}
  }

  def getTime(day:Int,getdata:Boolean = false)={
    val now = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date().getTime)
    //    println(now)
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, day)
    val time: Date = cal.getTime
    var newtime = ""
    if(getdata){
      newtime = new SimpleDateFormat("yyyy-MM-dd").format(time)
    }
    else{
      newtime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time)
    }
    newtime
  }


}
