package MoniFlowAnalyse

import java.io.{File, Serializable}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.SparkConf
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql._
import utils._
/**
  * Created by duan on 2017/2/24.
  */
object predict extends ConfSupport with HDFSUtils{
  val directory = new File("..")
  val filePath = directory.getAbsolutePath //设定为上级文件夹 获取绝对路径
  PropertyConfigurator.configure(filePath + "/conf/log4j.properties")
  val log = Logger.getLogger(this.getClass)

  def main(args: Array[String]) {
    log.error("<<<<<<<<<<<<<<<<<<<<<<<网络审计分析预测任务开始于" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + ">>>>>>>>>>>>>>>>>>>>>>")
    //加载配置文件
    loadconf("/models.conf")
    //初始化
   val sparkconf = new SparkConf().setMaster(getconf("moniflow.master")).setAppName(getconf("moniflow.appname"))
      .set("spark.port.maxRetries",getconf("moniflow.portmaxRetries"))
      .set("spark.cores.max",getconf("moniflow.coresmax"))
      .set("spark.executor.memory",getconf("moniflow.executormemory"))
    val sparkSession = SparkSession.builder().config(sparkconf).getOrCreate()

    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, -1)
    val time = cal.getTime
    val yestoday = new SimpleDateFormat("yyyy-MM-dd").format(time)
    val today = new SimpleDateFormat("yyyy-MM-dd").format(new Date)
    val respath=getconf("moniflow.res_path")
    val filename="T_MONI_FLOW_PREDICT@"+yestoday+"@MONI"

    try{
    //读取全量文件
    val path = getconf("moniflow.moniflow_path") + "/" + yestoday+"--" + today
//    val path=getconf("moniflow_path") + "/" + "2017-04-13"

    val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> path)
    val moniflowdata = sparkSession.read.options(options).format("com.databricks.spark.csv").load().filter("FLOWD='1'")

    val model=PipelineModel.load(getconf("moniflow.modelpp_path"))
    val RunPredict = new RunPredict(sparkSession,model,respath+filename)
    RunPredict.predict(moniflowdata)
    log.error("<<<<<<<<<<<<<<<<<<<<<<<网络审计分析预测任务结束于" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + ">>>>>>>>>>>>>>>>>>>>>>")
      sparkSession.stop()
  }catch {
     case e:Exception=>log.error(s"!!!!!!网络审计分析预测流程失败!!!!!!!!!!" + e.getMessage)
    }
    finally {
      sparkSession.stop()
    }
  }
}
class RunPredict(spark:SparkSession,model: PipelineModel,respath:String) extends Serializable with SaveUtils with urlUtil with LogSupport{
  import spark.implicits._
  def predict(moniflowdata: DataFrame) = {
    val divideddata=divideurl(spark,moniflowdata)
    var predictionDF = model.transform(divideddata).filter("predictionNB < 1")
    log.error("预测完毕>>>>>>>>>>>>>>>>>>>>>>>")
    predictionDF.show(10,false)
    if(predictionDF.take(10).length>1) {
      saveResult(predictionDF)
    }
  }
  def saveResult(predictionDF:Dataset[Row]) = {
    //预测结果
    val resDS=predictionDF.select($"id",$"date",$"srcip",$"dstip",$"uri",$"predictionNB",$"probabilityNB",$"predictionLR",$"probabilityLR").map{
      case Row(id:String,date:String,srcip:String,dstip:String,uri:String,predictionNB:Double,probabilityNB:org.apache.spark.ml.linalg.Vector,predictionLR:Double,probabilityLR:org.apache.spark.ml.linalg.Vector)=>
        RESMONI(id:String,date:String,srcip:String,dstip:String,uri:String,predictionNB.toString,probabilityNB(predictionNB.toInt).toString,predictionLR.toString,probabilityLR(predictionLR.toInt).toString)
    }
    log.error(">>>>>>>>>>>>>>>>>>>>>>>>>>最终结果>>>>>>>>>>>>>>>>>>>")
    resDS.show(5,false)
    log.error(respath)
    saveAsCSV(respath+".csv",resDS.toDF)
    val file = new File(respath+".flag").createNewFile()
  }
}



//val postgurl: String = postgprop.getProperty("aplication.sql.url")
//val user: String = postgprop.getProperty("aplication.sql.username")
//val password: String = postgprop.getProperty("aplication.sql.password")
//postgprop.put("user", user)
//postgprop.put("password", password)

//    resDF.write.mode(SaveMode.Append).jdbc(postgurl, "T_MONI_FLOW_predict", postgprop)
