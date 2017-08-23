package collaborativeFiltering

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{MinMaxScaler, StringIndexer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, SparkSession, _}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.Map



/**
  * Created by Administrator on 2017/5/2.
  */
class kmeansModel {

  def cluster(data:DataFrame,path:String,spark:SparkSession)={
    //data:IP、HOST、COUNT、IP_INDEX、HOST_INDEX
    //path:聚类结果存放路径
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>clusters start time:" + getNowTime()+">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    val sc = spark.sparkContext
    //创建映射
    val dataiphost = stringToIndexed(data)._1
    val hostIndex = stringToIndexed(data)._2
    println(hostIndex)
    //归一化
    val scldata = normalization2(dataiphost,spark)
    //构造稀疏向量
    val result = creatSqarseVector(dataiphost,scldata,spark)
    //聚类过程
    val clus = kMeansProcess(spark,result,hostIndex)
    //将聚类结果写入文件
    writedata(clus,path)
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>clusters end time:" + getNowTime()+">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
  }

  def stringToIndexed(data:DataFrame)={
    val indexer = new StringIndexer().setInputCol("IP").setOutputCol("IP_INDEX").fit(data)
    val dataip = indexer.transform(data)
    val indexiphost = new StringIndexer().setInputCol("HOST").setOutputCol("HOST_INDEX").fit(dataip)
    val dataiphost: DataFrame = indexiphost.transform(dataip)
    dataiphost.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //val converter = new IndexToString().setInputCol("categoryIndex").etOutputCol("originalCategory")//返回
    //打印每一列的数据类型
//    dataiphost.show(20)
//    dataiphost.printSchema()
    var hostIndex = scala.collection.mutable.Map[Double,String]()
    dataiphost.select("HOST","HOST_INDEX").rdd.foreach{row=>
      hostIndex += (row.getAs[Double]("HOST_INDEX") -> row.getAs[String]("HOST"))
    }
    (dataiphost,hostIndex)
  }

  def normalization2(dataiphost:DataFrame,spark:SparkSession)={
    val max = dataiphost.select("COUNT").rdd.map(_(0).toString.toDouble).max
    val min: Double = 0.0
    println("max:"+max)
    println("min:"+min)
    import spark.implicits._
    val result: DataFrame = dataiphost.rdd.map{ row =>
      val scl = (row.getAs[Double]("COUNT")-min)/(max-min)
      (row.getAs[String]("IP"),row.getAs[String]("HOST"),row.getAs[Double]("COUNT"),row.getAs[Double]("IP_INDEX"),row.getAs[Double]("HOST_INDEX"),scl)
    }.toDF("IP","HOST","COUNT","IP_INDEX","HOST_INDEX","SCLCOUNT")
    result.show(100,false)
    result
  }

  def normalization(dataiphost:DataFrame,spark:SparkSession)={
    import spark.implicits._
    val dataiphost1 = dataiphost.rdd.map{row=>
      (row(0).toString,row(1).toString,row(2).toString.toDouble,row(3).toString.toDouble,row(4).toString.toDouble,Vectors.dense(row(2).toString.toDouble))
    }.toDF("IP","HOST","COUNT","IP_COUNT","HOST_INDEX","COUNTS")
    println("归一化前：")
    dataiphost1.show(100,false)
    //归一化
    val scaler = new MinMaxScaler().setInputCol("COUNTS").setOutputCol("SCLCOUNT").setMin(0.0).fit(dataiphost1)
    val sclData: DataFrame = scaler.transform(dataiphost1)
    println("归一化后：")
    sclData.show(100,false)
    //返回结果DF
    val scldata = sclData.map{row=>
      var temp : List[Double] =Nil
      row.getAs[Vector]("SCLCOUNT") match {
        case vec:org.apache.spark.ml.linalg.Vector => temp = vec.toArray.toList
        case _ => 0.0
      }
      (row(0).toString,row(1).toString,row(2).toString.toDouble,row(3).toString.toDouble,row(4).toString.toDouble,temp(0))
    }.toDF("IP","HOST","COUNT","IP_INDEX","HOST_INDEX","SCLCOUNT")
    println("归一化后的数据:")
    scldata.show(20,false)
    scldata
  }

  def creatSqarseVector(dataiphost:DataFrame,scldata:DataFrame,spark:SparkSession)={
    //查看数据行、列长度
    import spark.implicits._
    val longip = dataiphost.select("IP_INDEX").toDF().map(row=>row(0).toString).map(_.toDouble.toInt).distinct.collect()
    val longhost = dataiphost.select("HOST_INDEX").toDF().map(row=>row(0).toString).map(_.toDouble.toInt).collect()
    //    println("ip max is :"+longip.collect().max)
    //    println("host max is :"+longhost.collect().max)
    //    longip.take(10).foreach(println)
    //构造稀疏向量
    val hostcount = longhost.count(_>=0)
    println("稀疏向量的长度:"+ hostcount)
    import spark.implicits._
    val modeldata = longip.map{row =>
      val tempdata = scldata.filter(s"IP_INDEX=$row.0")
      val vec: Array[Int] = tempdata.select("HOST_INDEX").collect().map(row=>row(0).toString.toDouble.toInt).toList.toArray
      val value: Array[Double] = tempdata.select("SCLCOUNT").collect().map(row=>row(0).toString.toDouble).toList.toArray
      //      println(vec.length)
      //      println(value.length)
      val IP = tempdata.select("IP").distinct().take(1)(0)(0).toString//.toString()
      (IP,Vectors.sparse(hostcount, vec, value))//.toDense
    }
    val result = spark.sparkContext.parallelize(modeldata).toDF("IP","FEATURES")
    println("输入聚类的数据:")
    result.show(false)
    result
  }

  def kMeansProcess(spark: SparkSession, dataDF: DataFrame,hostIndex:mutable.Map[Double, String]) = {
    val sc: SparkContext = spark.sparkContext
    val sql: SQLContext = spark.sqlContext
//    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>clusters start time:" + getNowTime()+">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    //聚类
    //设置k值
    var k =7
    val numIterations: Int = 20
    //判断k的取值是否适合做聚类
    val nData: Int = dataDF.count().toInt
    if (nData <= k & nData>2) {k = nData - 1}
    if (nData <= 2) {k = 1}//-----------------------------???
    if (nData > k) {k = k}
    println("聚类的 k:"+k)
    //=============
    val kMeansModel: KMeansModel = new KMeans().setK(k).setFeaturesCol("FEATURES").setPredictionCol("LABEL").fit(dataDF)//.setMaxIter(numIterations)
//    val kMeansModel=KMeans.train()
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
    println("离群簇："+target)
    //簇中心和簇点
    val clusterCenters = kMeansModel.clusterCenters
    val centersList = clusterCenters.toList
    val centersListBD = spark.sparkContext.broadcast(centersList)
    import spark.implicits._
    val kmeansTrainResult: DataFrame = result.rdd.map {
      row =>
        val centersListBDs: List[Vector] = centersListBD.value
        val IP = row.getAs[String]("IP")
        val FEATURES = row.getAs[Vector]("FEATURES")
//        val SCLFEATURES = row.getAs[Vector]("SCLFEATURES")
        var LABEL: String = row.getAs[Int]("LABEL").toString
        if (target.contains(row.getAs[Int]("LABEL"))) {
          LABEL = "other"
        }
        //给离群点打标签
        val CENTERPOINT: Vector = centersListBDs(row.getAs[Int]("LABEL"))
        //通过label获取中心点坐标
        val DISTANCE = math.sqrt(CENTERPOINT.toArray.zip(FEATURES.toArray).map(p => p._1 - p._2).map(d => d * d).sum).toString //距离
        (IP, LABEL, DISTANCE)
      }.toDF("IP","LABEL","DISTANCE")
//        (IP, FEATURES,LABEL, CENTERPOINT,DISTANCE)
//    }.toDF("IP","FEATURES","LABEL","CENTERPOINT","DISTANCE")
    //获取每一个簇中出现最多的10个HOST
//    kmeansTrainResult.map{row =>
//      val a = row.getAs[Vector]("FEATURES").toArray.sortWith(_.compare(_)>0).take(10)
//      hostIndex
//    }
    //新想法：按label筛选出数据，然后再将所有行FEATURES中HOST序号取出，groupby，寻找频数最大的10个
//    val label = kmeansTrainResult.select("LABEL").rdd.map(_.toString()).collect()
//    println("label:")
//    for(eachlabel <- label){
//      println("label:"+eachlabel)
//      val temp: Dataset[Vector] = kmeansTrainResult.filter(s"LABEL=$eachlabel").select("LABEL","FEATURES").map(row=>row.getAs[Vector]("FEATURES"))
//        .map{case vec:org.apache.spark.ml.linalg.Vector => vec.toArray
//        }
//      println()
//    }


    println("一级聚类结果：")
    kmeansTrainResult.show(10,false)
    kmeansTrainResult
  }

  def writedata(data:DataFrame,path:String) = {
    val saveOptions = Map("header" -> "true","delimiter"->"\t", "path" -> path)//"hdfs://172.16.12.38:9000/spark/modeldong/result"
    data.repartition(1).write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()//saveMode.Append 添加
    println("聚类结果数据写入成功，写入目录："+path)
//    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>clusters end time:" + getNowTime()+">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
  }

  def getNowTime(): String = {
    val format =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time = new Date().getTime
    format.format(time)
  }
}
