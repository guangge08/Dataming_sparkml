package AssetsPortrait

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.spark.SparkContext
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

import scala.collection.mutable
import org.apache.spark.sql.Column



/**
  * Created by Administrator on 2017/5/26.
  */
class kMeansModelClass extends Serializable{
  //聚类主函数
  def kMeansModel(spark: SparkSession, dataDF: DataFrame) = {
    val sc: SparkContext = spark.sparkContext
    val sql: SQLContext = spark.sqlContext
    val starttime = getNowTime()
    println("first clusters start time:" + starttime)
    //聚类
    //    val silhouetteCoefficient1 = new silhouetteCoefficient1()
    //设置k值
    var k =4 //传入轮廓系数前的k
    val numIterations: Int = 20//迭代次数
    //判断k的取值是否适合做聚类
    val nData: Int = dataDF.count().toInt
    if (nData <= k & nData>2) {k = nData - 1}
    if (nData <= 2) {k = 1}//-----------------------------???
    if (nData > k) {k = k}
    println("第一次聚类的best k:"+k)
    //=============
    val kMeansModel: KMeansModel = new KMeans().setK(k).setFeaturesCol("SCLFEATURES").setPredictionCol("LABEL").fit(dataDF)//.setMaxIter(numIterations)
    val result: DataFrame = kMeansModel.transform(dataDF)//DF
    //
    println("处理一级聚类离群点："+ getNowTime())
    var target:List[Int] = Nil
    //    println("k:"+k)
    (0 to k-1).toList.foreach { eachk =>
      if(result.filter(s"LABEL='$eachk'").count()<2){
        target = target :+ eachk
        println("簇："+eachk+"含有的数据count："+result.filter(s"LABEL='$eachk'").count())
      }
    }
    println("离群点："+target)
    //簇中心和簇点
    val clusterCenters = kMeansModel.clusterCenters
    val centersList = clusterCenters.toList
    val centersListBD = spark.sparkContext.broadcast(centersList)
    import spark.implicits._
    val kmeansTrainOne: DataFrame = result.rdd.map {
      row =>
        val centersListBDs: List[Vector] = centersListBD.value
        val IP = row.getAs[String]("IP")
        val FEATURES = row.getAs[Vector]("FEATURES")
        val SCLFEATURES = row.getAs[Vector]("SCLFEATURES")
        var LABEL:String = row.getAs[Int]("LABEL").toString
        if(target.contains(row.getAs[Int]("LABEL"))){LABEL = "other"}//给离群点打标签
      val CENTERPOINT: Vector = centersListBDs(row.getAs[Int]("LABEL"))//通过label获取中心点坐标
      val DISTANCE = math.sqrt(CENTERPOINT.toArray.zip(SCLFEATURES.toArray).map(p => p._1 - p._2).map(d => d * d).sum).toString//距离
        (IP,FEATURES,SCLFEATURES,LABEL,CENTERPOINT,DISTANCE)
    }.toDF("IP", "FEATURES", "SCLFEATURES", "LABEL", "CENTERPOINT", "DISTANCE")
    println("聚类结果：")
    println("first clusters end time:" + getNowTime())
    kmeansTrainOne.show(10)
    println("聚类结束-------------------------------------------------")
    kmeansTrainOne
  }

  def getNowTime(): String = {
    val format =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time = new Date().getTime
    format.format(time)
  }

  def ReturnCenterFirst(spark: SparkSession, kMeansOneDF: DataFrame) ={
    //定义一个空的map
    var CenterMap = mutable.Map.empty[Int, List[Int]]
    //创建表名
    val sql: SQLContext = spark.sqlContext
    kMeansOneDF.createOrReplaceTempView("BIGDATA")
    val predictionnum: Array[Int] = sql.sql(s"SELECT DISTINCT LABEL FROM BIGDATA WHERE LABEL<>'other'").select("LABEL").rdd.map(r => r(0).toString.toDouble.toInt).collect()
    for (label <- predictionnum) {
      //簇点长度
      var FeaturesLen: Int = sql.sql("SELECT COUNT(*) FROM BIGDATA WHERE LABEL='" + label.toString + "'").collect().head.getLong(0).toInt
      //计算中心坐标//归一化前的特征
      var FeaturesList= sql.sql("SELECT FEATURES FROM BIGDATA WHERE LABEL='" + label.toString + "'").select("FEATURES").rdd
        .map {
          line =>
            var tmp: List[Double] = Nil
            line(0) match {
              case vec: org.apache.spark.ml.linalg.Vector => tmp = vec.toArray.toList
              case _ => 0
            }
            tmp
        }.reduce {
        (x, y) =>
          x.zip(y).map(p => (p._1 + p._2))
      }.map(p => p / FeaturesLen)
      //构造成一个map
      CenterMap += (label -> FeaturesList.map(_.toInt))//label->List(中心点反归一化的值)
    }

    println(CenterMap)
    import spark.implicits._
    val ResultDF= kMeansOneDF.rdd.map {
      row =>
        val IP = row.getAs[String]("IP")
        val FEATURES = row.getAs[Vector]("FEATURES")
        val SCLFEATURES = row.getAs[Vector]("SCLFEATURES")
        val LABEL = row.getAs[String]("LABEL")
        val CENTERPOINT = row.getAs[Vector]("CENTERPOINT")
        val DISTANCE = row.getAs[String]("DISTANCE")
        //反归一化的中心点
        var RETURNCENTER :List[Int]= Nil
        if (LABEL=="other"){
          RETURNCENTER = RETURNCENTER :+ -1
          //          RETURNCENTER = RETURNCENTER ++ SCLFEATURES.toArray.toList.map(_.toInt)
        }else {
          RETURNCENTER = CenterMap(LABEL.toDouble.toInt)
        }
//        (IP, FEATURES, SCLFEATURES, LABEL, CENTERPOINT, RETURNCENTER.mkString("#"), DISTANCE)
//    }.toDF("IP", "FEATURES", "SCLFEATURES", "LABEL", "CENTERPOINT","RETURNCENTER", "DISTANCE")
        (IP, FEATURES, SCLFEATURES, LABEL, RETURNCENTER.mkString("#"), DISTANCE)
    }.toDF("IP", "FEATURES", "SCLFEATURES", "LABEL","RETURNCENTER", "DISTANCE")
//    println("反归一化数据:")
//    ResultDF.show(10)
//    ResultDF.printSchema()
    ResultDF
  }


  def addLabelUseMean(data:DataFrame,spark:SparkSession,sql:SQLContext,properties:Properties)= {
    import spark.implicits._
    val w1 = List("weight.proto.tcp","weight.proto.udp","weight.proto.icmp","weight.proto.gre").map(properties.getProperty(_).toDouble)
    val w2 = List("weight.flow.packernum","weight.flow.bytesize").map(properties.getProperty(_).toDouble)
    //新增两列 PROTO和FLOW
    val dataST = data.rdd.map { line =>
      val IP = line.getAs[String]("IP")
      val FEATURES = line.getAs[Vector]("FEATURES")
      val SCLFEATURES = line.getAs[Vector]("SCLFEATURES")
      val FEATURE = SCLFEATURES.toArray
      val LABEL = line.getAs[String]("LABEL")
      val RETURNCENTER = line.getAs[String]("RETURNCENTER")
      val RETURNCENTERVEC = Vectors.dense(line.getAs[String]("RETURNCENTER").split("#").map(_.toDouble))
      val DISTANCE = line.getAs[String]("DISTANCE")
      val proto = w1(0) * FEATURE(0) + w1(1) * FEATURE(1) + w1(2) * FEATURE(2) + w1(3) * FEATURE(3)
      val flow = w2(0) * FEATURE(5) + w2(1) * FEATURE(6)
      (IP, FEATURES,LABEL,RETURNCENTER,RETURNCENTERVEC,DISTANCE,proto,flow)
    }.toDF("IP", "FEATURES", "LABEL", "RETURNCENTER", "RETURNCENTERVEC", "DISTANCE", "PROTO", "FLOW")
    //计算两个特征（PROTO、FLOW）的均值
    val colmean: List[Double] = List("PROTO","FLOW").map(dataST.select(_).rdd.map(_(0).toString.toDouble).mean())
    println("complete:calculated the mean values of proto and flow, E(proto)="+colmean(0)+"\tE(flow)="+colmean(1))
    //将数据分成两份（other和非other标签）
    val data_other = dataST.filter("LABEL='other'").toDF()
    val data_except_ohter = dataST.filter("LABEL!='other'").toDF()
    ////非other数据操作
    //归一化
//    val scaler = new MinMaxScaler().setMin(0.0).setInputCol("RETURNCENTERVEC").setOutputCol("SCLRETURNCENTERVEC").fit(data_except_ohter)
//    val sclData: DataFrame = scaler.transform(data_except_ohter)
//    println("归一化:")
//    sclData.show(10,false)
    //归一化2
    val minmax = data.select("FEATURES").rdd.map(line=>line.getAs[Vector]("FEATURES").toArray)
    val tcp = minmax.map(line=>line(0)).max()
    val udp = minmax.map(line=>line(1)).max()
    val icmp = minmax.map(line=>line(2)).max()
    val gre = minmax.map(line=>line(3)).max()
    val packernum = minmax.map(line=>line(5)).max()
    val bytesize = minmax.map(line=>line(6)).max()
    //筛选数据(遍历label)
    val label = data_except_ohter.select("LABEL").rdd.distinct().map(_(0).toString).collect()
    val listdata = label.map{ each =>
      data_except_ohter.filter(s"LABEL=$each").select("LABEL","RETURNCENTER").distinct().rdd.map{line =>
        val returncenter = line.getAs[String]("RETURNCENTER").split("#").map(_.toDouble)
        var protolabel = ""
        var flowlabel = ""
        val proto = w1(0) * minmaxscala(returncenter(0),tcp) + w1(1) * minmaxscala(returncenter(1),udp) + w1(2) * minmaxscala(returncenter(2),icmp) + w1(3) * minmaxscala(returncenter(3),gre)
        val flow = w2(0) * minmaxscala(returncenter(5),packernum) + w2(1) * minmaxscala(returncenter(6),bytesize)
        if(proto>colmean(0)){protolabel = "high"}
        else{protolabel = "low"}
        if(flow>colmean(1)){flowlabel = "high"}
        else{flowlabel = "low"}
        val addlabel = protolabel + "#" + flowlabel
        (line.getAs[String]("LABEL"),line.getAs[String]("RETURNCENTER"),addlabel)
      }.toDF("LABEL","RETURNCENTER","ADDLABEL")
    }
    //将所有label-data纵向连接
    val unionFun = (a:DataFrame, b:DataFrame) =>a.union(b).toDF
    val uniondata = listdata.tail.foldRight(listdata.head)(unionFun)
    //与原始数据关联
    uniondata.createOrReplaceTempView("addlabeldata")
    data_except_ohter.createOrReplaceTempView("origindata")
    val resultdata = spark.sql("select t.*,s.ADDLABEL from origindata t left join addlabeldata s on t.LABEL = s.LABEL")
    println("数据添加标签:")
    resultdata.show(10,false)
    ////other数据操作
    val resultdataother = data_other.withColumn("ADDLABEL",data_other.col("LABEL"))
    resultdata.union(resultdataother).show(10,false)
    resultdata.union(resultdataother).select("IP","FEATURES","LABEL","RETURNCENTER","DISTANCE","ADDLABEL")
  }

  def minmaxscala(x:Double,max:Double,min:Double=0.0)={
    x-min/(max-min)
  }

}
