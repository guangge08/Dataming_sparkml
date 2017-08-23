package MoniFlowAnalyse

import java.net.URLDecoder

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.RAWMONI

/**
  * Created by duan on 2017/6/13.
  */
trait urlUtil {
  def divideurl(spark:SparkSession,moniflowdata: DataFrame) = {
    lazy val log = Logger.getLogger(this.getClass)
    import spark.implicits._
    val divideddata = moniflowdata.mapPartitions {
      partIt=>
        val partres=partIt.map {
          line =>
            val preuri = line.getAs[String]("URI")
            var tmp = RAWMONI("", "2017-01-01 00:00:00", "", "", "", "")
            if(preuri!=null) {
              try {
                val uri = if (preuri.contains("%")) URLDecoder.decode(preuri, "UTF-8") else preuri
                var urid:String = ""
                if (uri.contains("?") && uri.contains("=")) {
                  urid = uri.map{
                    case '/'=>" / "
                    case '='=>" = "
                    case '?'=>" ? "
                    case '%'=>" % "
                    case '.'=>" . "
                    case '-'=>" - "
                    case '_'=>" _ "
                    case '&'=>" & "
                    case ':'=>" : "
                    case '''=>" ' "
                    case '('=>" ( "
                    case ')'=>" ) "
                    case ','=>" , "
                    case '|'=>" | "
                    case '*'=>" * "
                    case '+'=>" + "
                    case ';'=>" ; "
                    case s:Char=>s
                  }.mkString("")
                  if (urid.head == ' ') {
                    urid = urid.drop(1)
                  }
                  tmp = RAWMONI(line.getAs[String]("ROW"), line.getAs[String]("DATE"), line.getAs[String]("SRCIP"), line.getAs[String]("DSTIP"),uri,urid)
                }
              }
              catch {
                case e: Exception => {
                  log.error(s"!!!!!!!!!!!!url解析失败!!!!!!!!!!!"+"错误rowkey为>>>>>>>>>>>"+line.getAs[String]("ROW") +"\t"+ e.getMessage +"\t"+ "错误url为>>>>>>>>>>>" + preuri)
                }
              }
            }
            tmp
        }
        partres

    }.filter(_.id.length > 0)
    divideddata
  }
}
