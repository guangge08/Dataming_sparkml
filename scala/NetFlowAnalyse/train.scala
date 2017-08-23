package NetFlowAnalyse
import java.io.File
import java.sql.Timestamp
import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.Date

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import utils.{ConfSupport, HbaseAPI, LogSupport, TrainNET}

/**
  * Created by duan on 2017/5/11.
  */
object train extends HbaseAPI with ConfSupport with LogSupport{
  val directory = new File("..")
  val filePath = directory.getAbsolutePath //设定为上级文件夹 获取绝对路径
  PropertyConfigurator.configure(filePath + "/conf/log4j.properties")
  def main(args: Array[String]) {
    log.error("<<<<<<<<<<<<<<<<<<<<<<<流量分析训练任务开始于" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + ">>>>>>>>>>>>>>>>>>>>>>")
    //加载配置文件
    loadconf("/models.conf")
    val sparkconf = new SparkConf()
      .setMaster(getconf("netflow.master"))
      .setAppName(getconf("netflow.appname"))
      .set("spark.port.maxRetries",getconf("netflow.portmaxRetries"))
      .set("spark.cores.max",getconf("netflow.coresmax"))
      .set("spark.executor.memory",getconf("netflow.executormemory"))

    val spark = SparkSession.builder().config(sparkconf).getOrCreate()
    try{
    //读取训练数据并做转换
    val netflowDF =maketraindata(spark,getconf("netflow.nsamplepath"),getconf("netflow.zokhost"),getconf("netflow.zokport"))
    netflowDF.persist(StorageLevel.MEMORY_AND_DISK)
    //转换列的类型
    val preDF=netflowDF.select(
      netflowDF("ATTACKTYPE").cast("Double"),
      netflowDF("SPANDTIME").cast("Double"),
      netflowDF("TCP").cast("Double"),
      netflowDF("UDP").cast("Double"),
      netflowDF("PACKERNUM").cast("Double"),
      netflowDF("BYTESIZE").cast("Double"),
      netflowDF("FLOWDIRECTION").cast("Double"))
    //构造特征向量
    val assembler = new VectorAssembler().setInputCols(Array("SPANDTIME", "TCP", "UDP", "PACKERNUM", "BYTESIZE")).setOutputCol("features")
    //识别因子型特征
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(6)
    //随机森林
    //模型配置
    val numtrees=getconf("netflow.NumTrees").toInt
    val impurity=getconf("netflow.Impurity")
    val maxbins=getconf("netflow.MaxBins").toInt
    val maxdepth=getconf("netflow.MaxDepth").toInt
    val subsamplingrate=getconf("netflow.SubsamplingRate").toDouble
    val FeatureSubsetStrategy=getconf("netflow.FeatureSubsetStrategy")
    val rfmodel = new RandomForestClassifier()
      .setLabelCol("ATTACKTYPE")//预测列名
      .setFeaturesCol("indexedFeatures")//特征列名
      .setNumTrees(numtrees)//树的数量
      .setImpurity(impurity)//信息增益准则
      .setMaxBins(maxbins)//连续特征离散化的最大数量，以及选择每个节点分裂特征的方式
      .setMaxDepth(maxdepth)//树的最大深度
      .setSubsamplingRate(subsamplingrate)//每棵树所用的训练数据的比例
      .setFeatureSubsetStrategy(FeatureSubsetStrategy)//每棵树所用的特征数

    //用留出法做交叉验证
    val Array(trainingData, testingData) = preDF.randomSplit(Array(0.7, 0.3))
    //流水线作业
    val pipeline = new Pipeline().setStages(Array(assembler, featureIndexer,rfmodel))
    val model = pipeline.fit(trainingData)
    //保存模型
    model.write.overwrite.save(getconf("netflow.modelpp_path"))
    //预测作为验证
    val predictions = model.transform(testingData)
    log.error(">>>>>>>>>>预测结果>>>>>>>")
    predictions.show(5,false)
    log.error("准确率为"+new DecimalFormat("#00.000").format(100.0 * predictions.filter(row=>row.getAs("ATTACKTYPE")==row.getAs("prediction")).count / testingData.count) +"%")
    log.error("<<<<<<<<<<<<<<<<<<<<<<<流量分析训练任务结束于" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + ">>>>>>>>>>>>>>>>>>>>>>")
  }catch {
      case e: Exception => {
        log.error(s"!!!!!!流量分析训练任务流程失败!!!!!!!!!!" + e.getMessage)
      }
    }
    finally {
      spark.stop()
    }
  }
  def maketraindata(spark:SparkSession,NSamplepath:String,zokHOST:String,zokPORT:String)={
    log.error(">>>>>>>>>>>>>开始生成训练数据>>>>>>>>>>>")
    val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> NSamplepath)
    val NSampleDF = spark.read.options(options).format("com.databricks.spark.csv").load()
    log.error(">>>>>>>>>>>>>负样本>>>>>>>>>>>")
    NSampleDF.show(5,false)
    val NSamplenum=NSampleDF.count.toInt
    log.error(">>>>>负样本数量"+NSamplenum)
    log.error(">>>>>>>>>>>>>正样本>>>>>>>>>>>")
    log.error(">>>>>zookeeper主机名"+zokHOST)
    log.error(">>>>>zookeeper端口"+zokPORT)
    val alldata=getNETFLOWRows(spark,zokHOST,zokPORT).limit(NSamplenum)
    alldata.createOrReplaceTempView("tmp")
    import spark.implicits._
    val PSampleDF=spark.sql("""select '0' as ATTACKTYPE,
                                     |tmp.STARTTIME,
                                     |tmp.ENDTIME,
                                     |case when tmp.PROTO='TCP' then '1' else '0' end as TCP,
                                     |case when tmp.PROTO='UDP' then '1' else '0' end as UDP,
                                     |tmp.PACKERNUM,tmp.BYTESIZE,'2' as FLOWDIRECTION,
                                     |'0' as SAME,tmp.ROW as RECORDID,tmp.SRCIP,tmp.DSTIP
                                     | from tmp""".stripMargin).map{
          case Row(attacktype:String,starttime:String,endtime:String,tcp:String,udp:String,packernum:String,bytesize:String,flowdirection:String,same:String,recordid:String,srcip:String,dstip:String)=>
            TrainNET(attacktype,(Timestamp.valueOf(endtime).getTime-Timestamp.valueOf(starttime).getTime).toString,tcp,udp,packernum,bytesize,flowdirection,same,recordid,srcip,dstip)
        }.toDF
    PSampleDF.show(5,false)
    NSampleDF.union(PSampleDF)
  }
}