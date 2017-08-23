package AnomalyDetection

import java.io.File

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import utils._

/**
  * Created by duan on 2017/7/14.
  */
object EnsembleClustering extends HbaseAPI with SaveUtils with ConfSupport with timeSupport with LogSupport with HDFSUtils{
  val directory = new File("..")
  val filePath = directory.getAbsolutePath //设定为上级文件夹 获取绝对路径
  PropertyConfigurator.configure(filePath + "/conf/log4j.properties")//读取日志配置文件
  def main(args: Array[String]) = {
    //加载配置
    loadconf("/models.conf")
    //配置spark
    val sparkconf = new SparkConf()
      .setMaster(getconf("anomalyDetection.master"))
      .setAppName(getconf("anomalyDetection.appname"))
      .set("spark.port.maxRetries",getconf("anomalyDetection.portmaxRetries"))
      .set("spark.cores.max",getconf("anomalyDetection.coresmax"))
      .set("spark.executor.memory",getconf("anomalyDetection.executormemory"))
    //启动spark
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    val Options = Map("header" -> "true", "delimiter" -> "\t")
    val rawDF=spark.read.options(Options).csv(getconf("anomalyDetection.netflow_path"))
    rawDF.createOrReplaceTempView("t")
    //筛选特定ip
    // 前三个字段 流的寿命与质量 影响力 反映对资源的占用情况
    // 后三个字段 流的速度与变化程度  扩散力 反映流的传播能力
    val netDF=spark.sql(
      """
        |SELECT SRCIP,DSTIP,PROTO,RECORDTIME,SUM(COSTTIME) AS COSTTIME,
        |SUM(PACKERNUM) AS PACKERNUM,SUM(BYTESIZE) AS BYTESIZE,
        |COUNT(*) AS FLOWNUMA,
        |COUNT(DISTINCT srcport) AS FLOWNUMS,
        |COUNT(DISTINCT dstport) AS FLOWNUMD,
        |CONCAT_WS(',',COLLECT_SET(DISTINCT SRCPORT)) AS SRCPORTS,
        |CONCAT_WS(',',COLLECT_SET(DISTINCT DSTPORT)) AS DSTPORTS FROM t
        |GROUP BY RECORDTIME,SRCIP,DSTIP,PROTO
      """.stripMargin)
    //"WHERE SRCIP='221.176.65.9' OR DSTIP='221.176.65.9'"
    netDF.show(10,false)
    netDF.persist(StorageLevel.MEMORY_AND_DISK)
    //分别使用三个字段，做集成聚类
    val netModel=new AnomalyDetection(netDF,Array("COSTTIME","PACKERNUM", "BYTESIZE"),"1")
    val netResDF=netModel.ComputeOutliers
    val flowModel=new AnomalyDetection(netResDF,Array("FLOWNUMA","FLOWNUMS", "FLOWNUMD"),"2")
    val flowResDF=flowModel.ComputeOutliers
    //算出最终的离群值
    val unusualsUdf=org.apache.spark.sql.functions.udf(
      (UNUSUAL1:Double,UNUSUAL2:Double)=>{
        UNUSUAL1+UNUSUAL2
      }
    )
    val resultDF=flowResDF.withColumn("UNUSUALS",unusualsUdf(flowResDF("UNUSUAL1"),flowResDF("UNUSUAL2")))
    println(">>>>>>>>>最终结果")
    //选出关心字段，并排序
    val endDF=resultDF.select("SRCIP","DSTIP","PROTO","RECORDTIME","COSTTIME","PACKERNUM","BYTESIZE","FLOWNUMA","FLOWNUMS","FLOWNUMD","SRCPORTS","DSTPORTS","NUM1","UNUSUAL1","NUM2","UNUSUAL2","UNUSUALS")
      .orderBy(- resultDF("UNUSUALS"))
    endDF.show(10,false)
    //保存在hdfs
    val saveOptions = Map("header" -> "true", "delimiter" -> "\t")
    endDF.write.mode(SaveMode.Overwrite).options(saveOptions).csv(getconf("anomalyDetection.res_path"))
    //生成本地文件
    val resultFile=new File(getconf("anomalyDetection.res_local"));resultFile.deleteOnExit()//删除已有文件避免重复
    merge(getconf("anomalyDetection.res_path"),getconf("anomalyDetection.res_local"))
  }
}






//异常检测的类 参数为异常检测的数据和特征
class AnomalyDetection(dataDF:DataFrame,features:Array[String],id:String){
  //生成字段名
  val vecFeatureCol="VECFEATURES"+id //初始的特征向量
  val featureCol="FEATURES"+id       //归一化后的特征向量
  val labelCol="LABEL"+id            //kmeans完成后获得的类别标号
  val centerCol="CENTER"+id          //对应的中心点坐标
  val numCol="NUM"+id                //对应类别中的样本数量
  val distanceCol="DISTANCE"+id      //与对应中心点的距离
  val disFeatureCol="DISFEATURES"+id //距离和样本数量构成的特征向量
  val vecUnusualCol="VECUNUSUAL"+id  //归一化后的特征向量
  val unusualCol="UNUSUAL"+id        //离群值
  //数据预处理 生成特征向量、归一化  注意，这个归一化应该使用很长的时间范围得到一个同一个标准
  def dataPrepare ={
    val assembler = new VectorAssembler().setInputCols(features).setOutputCol(vecFeatureCol)
    val scaler = new MinMaxScaler().setInputCol(vecFeatureCol).setOutputCol(featureCol)
    val pipelineModel = new Pipeline().setStages(Array(assembler,scaler)).fit(dataDF)
    val preDF=pipelineModel.transform(dataDF)
    preDF
  }
  //使用kmeans模型
  def kmeans(preDF:DataFrame)={
    //先选一个合适的k值
    var computeCostFormer=0.0
    var computeCostLatter=0.0
    var flag=true
    var bestk=0
    //从2到20逐渐去找合适的k
    for(k<-2 to 20 if flag) {
      val oneOfModel = new KMeans().setK(k).setFeaturesCol(featureCol).setPredictionCol(labelCol).setMaxIter(10).fit(preDF)
      computeCostLatter=oneOfModel.computeCost(preDF)
      println("-------------------------------------------------")
      println("VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV")
      println(">>>>当前k为:"+k)
      println(">>>>当前的内聚度为："+computeCostLatter)
      println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
      if(computeCostLatter>computeCostFormer && k>2){
        bestk=k-1
        println("****************最佳k为"+bestk)
        flag=false
      }
      computeCostFormer=computeCostLatter
    }
    //如果在2到20之间找不到合适的k，就指定为2
    if(bestk==0){bestk=2}
    //得到最优的模型
    val kMeansModel=new KMeans().setK(bestk).setFeaturesCol(featureCol).setPredictionCol(labelCol).setMaxIter(10).fit(preDF)
    println("最终模型的k值为"+kMeansModel.getK)
    //使用模型
    val endDF=kMeansModel.transform(preDF)
    endDF.show(10,false)
    (kMeansModel,endDF)
  }

  //计算离群值
  def ComputeOutliers={
    val (kMeansModel,endDF)=kmeans(dataPrepare)
    //得到每个点对应中心点的归属
    val centers=kMeansModel.clusterCenters
    val centerUdf=org.apache.spark.sql.functions.udf(
      (label:String)=>{
        centers(label.toDouble.toInt)
      }
    )
    //得到簇内点的数量
    val clusterNum =for(i<-0 until  centers.length) yield {
      endDF.filter((labelCol+"="+i)).count.toInt
    }
    val clusterNumUdf=org.apache.spark.sql.functions.udf(
      (label:String)=>{
        clusterNum(label.toDouble.toInt)
      }
    )
    //算出各个点与各自中心点的距离
    val distanceUdf=org.apache.spark.sql.functions.udf(
      (locSelf:Vector,locCenter:Vector,label:String)=>{
        val distance=math.sqrt(locSelf.toArray.zip(locCenter.toArray).//计算两向量之间的距离
          map(p => p._1 - p._2).map(d => d*d).sum)
        var distantArray=new Array[Double](centers.length)//创建一个各个点都能用的数组
        distantArray(label.toDouble.toInt)=distance//填入距离值
        org.apache.spark.ml.linalg.Vectors.dense(distantArray)//转化为向量
      }
    )
    val withCenterDF=endDF.withColumn(centerCol,centerUdf(endDF(labelCol))).withColumn(numCol,clusterNumUdf(endDF(labelCol)))
    val withDistanceDF=withCenterDF.withColumn(distanceCol,distanceUdf(withCenterDF(featureCol),withCenterDF(centerCol),withCenterDF(labelCol)))
    println(">>>>>>>>>>>带上中心点的DF>>>>>>>>>")
    withCenterDF.show(3,false)
    println(">>>>>>>>>>>带上距离的DF>>>>>>>>>")
    withDistanceDF.show(3,false)
    //对距离和簇内点数量进行归一化
    val assemblerDis = new VectorAssembler().setInputCols(Array(distanceCol,numCol)).setOutputCol(disFeatureCol)
    val scalerDis = new MinMaxScaler().setInputCol(disFeatureCol).setOutputCol(vecUnusualCol)
    val pipelineDis = new Pipeline().setStages(Array(assemblerDis,scalerDis)).fit(withDistanceDF)
    val DisDF=pipelineDis.transform(withDistanceDF)
    println(">>>>>>>>>>将距离和簇点数量归一化>>>>>>>>")
    DisDF.show(3,false)

    //计算离群值
    val unusualUdf=org.apache.spark.sql.functions.udf(
      (vecUnusual:Vector,label:String)=>{
        val distance=vecUnusual(label.toDouble.toInt)
        val number=1-vecUnusual.toArray.last
        distance*distance+number*number
      }
    )
    println(">>>>>>>>>>算出离群值>>>>>>>>")
    val resDF=DisDF.withColumn(unusualCol,unusualUdf(DisDF(vecUnusualCol),DisDF(labelCol)))
    resDF.show(3,false)
    resDF
  }
}
