package MoniFlowAnalyse

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.{LogisticRegression, NaiveBayes}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import utils._

/**
  * Created by duan on 2017/2/24.
  */
object train extends ConfSupport{
  val directory = new File("..")
  val filePath = directory.getAbsolutePath //设定为上级文件夹 获取绝对路径
  PropertyConfigurator.configure(filePath + "/conf/log4j.properties")
  val log=Logger.getLogger(this.getClass)
  //加载配置文件
  loadconf("/models.conf")
  def main(args : Array[String]) {

    //初始化
    val sparkconf = new SparkConf()
      .setMaster(getconf("moniflow.master"))
      .setAppName(getconf("moniflow.appname"))
      .set("spark.port.maxRetries",getconf("moniflow.portmaxRetries"))
      .set("spark.cores.max",getconf("moniflow.coresmax"))
      .set("spark.executor.memory",getconf("moniflow.executormemory"))
    val sparkSession = SparkSession.builder().config(sparkconf).getOrCreate()
    val sc = sparkSession.sparkContext
    try {
      log.error("<<<<<<<<<<<<<<<<<<<<<<<网络审计分析训练任务开始于" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + ">>>>>>>>>>>>>>>>>>>>>>")
      //读取训练数据
      val TrainModel = new TrainModel(sparkSession)
      TrainModel.train
      log.error("<<<<<<<<<<<<<<<<<<<<<<<网络审计分析训练任务结束于" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + ">>>>>>>>>>>>>>>>>>>>>>")
      sc.stop()
      sparkSession.stop()
    }catch {
    case e: Exception => {
      log.error(s"!!!!!!网络审计分析训练流程失败!!!!!!!!!!" + e.getMessage)
    }
  }
    finally {
      sc.stop()
      sparkSession.stop()
    }
  }
}
class TrainModel(spark:SparkSession) extends PerformanceMeasure with urlUtil with HbaseAPI with ConfSupport with LogSupport{
  import spark.implicits._
  loadconf("/models.conf")
  //更新训练数据
  def maketraindata()={
    val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> getconf("moniflow.nsamplepath"))
    val NSampleDF = spark.read.options(options).format("com.databricks.spark.csv").load()
    log.error(">>>>>>>>>>>>>负样本>>>>>>>>>>>")
    NSampleDF.show(5,false)
    val NSamplenum=NSampleDF.count.toInt
    log.error(">>>>>负样本数量"+NSamplenum)
    log.error(">>>>>>>>>>>>>正样本>>>>>>>>>>>")
    val zokhost=getconf("moniflow.zokhost");log.error(">>>>>zookeeper主机名"+zokhost)
    val zokport=getconf("moniflow.zokport");log.error(">>>>>zookeeper端口"+zokport)
    val moniflowDF=divideurl(spark,getMONIFLOWRows(spark,zokhost,zokport).toDF).toDF.limit(NSamplenum*10)
    val PSampleDF=moniflowDF.map{
      case Row(id:String,date:String,srcip:String,dstip:String,uri:String,urltext:String)=>
        TrainMONI("1",urltext)
    }.toDF
    PSampleDF.show(5,false)
    NSampleDF.union(PSampleDF)
  }
  //设置特征数量
  var featuresNUM:Int=20000
  def train={
    val allDataDFP=maketraindata
    allDataDFP.persist(StorageLevel.MEMORY_AND_DISK)
    log.error(">>>>>>>>>>>>>>>>>>>>样本获取完毕>>>>>>>>>>>>>>>>>>>>>>>")
    val allDataDF=allDataDFP.select(allDataDFP("label").cast("Double"),allDataDFP("urltext"))
    val splits=allDataDF.randomSplit(Array(0.7,0.3))
    val (trainingDF,testingDF) = (splits(0),splits(1))
    //将词语转换成数组
    val time1=System.currentTimeMillis()
    var tokenizer = new Tokenizer().setInputCol("urltext").setOutputCol("urlwords")
    //计算每个词在文档中的词频 TF
    val time2=System.currentTimeMillis()
    var hashingTF = new HashingTF().setInputCol("urlwords").setOutputCol("rawFeatures").setNumFeatures(featuresNUM)
    //计算每个词逆向文件频率 IDF
    val time3=System.currentTimeMillis()
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    //朴素贝叶斯模型 逻辑回归模型
    val nbModel =new NaiveBayes().setFeaturesCol("features").setPredictionCol("predictionNB").setProbabilityCol("probabilityNB").setRawPredictionCol("rawpredictionNB")
    val lrModel = new LogisticRegression()
      .setMaxIter(getconf("moniflow.MaxIter").toInt)//最大迭代次数 //100
      .setRegParam(getconf("moniflow.RegParam").toDouble)//正则化参数 //0.05
      .setElasticNetParam(getconf("moniflow.ElasticNetParam").toDouble)//网格参数 //0.01
      .setFeaturesCol("features").setPredictionCol("predictionLR").setProbabilityCol("probabilityLR").setRawPredictionCol("rawpredictionLR")
    //流水线作业
    val pipeline = new Pipeline().setStages(Array(tokenizer,hashingTF,idf,nbModel,lrModel))
    val model= pipeline.fit(trainingDF);savemodel(model)
    val testpredictionAndLabel=model.transform(testingDF)
    testpredictionAndLabel.show(10)
    testpredictionAndLabel.printSchema()
    //获得混淆矩阵
    val cmDFLR=getconfusionMatrix(testpredictionAndLabel,spark)
    cmDFLR.show(false)
    cmDFLR.foreach{line=>val log=Logger.getLogger(this.getClass);log.error(line)}
  }
  def savemodel(model: PipelineModel): Unit ={
    val pathpp=getconf("moniflow.modelpp_path")
    //保存模型
    model.write.overwrite.save(pathpp)
    log.error("保存成功")
  }
}