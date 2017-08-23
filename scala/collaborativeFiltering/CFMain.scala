package collaborativeFiltering

import collaborativeFiltering.CFCalculateSimilarity._
import collaborativeFiltering.CFCalucateRating._
import collaborativeFiltering.CFDataProcess._
import collaborativeFiltering.CFEnvironmentSetup._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.storage.StorageLevel

/**
  * Created by Administrator on 2017/4/24.
  */
object CFMain {

  object localMode{
    val getSpark = getLocalSparkSession()
    val originalDataPath = "file:///G://landun//蓝基//moniflowData_from_hadoop//20170413"
    val storePath = "G://landun//蓝基//storage//result"
  }
  object hdfsMode{
    val getSpark = getSparkSession()
    val originalDataPath  = "hdfs://172.16.12.38:9000/spark/moniflowHDFS/2017-04-13"
      val storePath = "G://landun//蓝基//storage//result"

  }


  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN) //close info
    //配置
    val runMode = localMode//选择运行模式
    val spark: SparkSession = runMode.getSpark
    val sc: SparkContext = spark.sparkContext
    val sql: SQLContext = spark.sqlContext

    //数据处理
    val originalDataPath = runMode.originalDataPath
    val storePath = runMode.storePath


    //limitOption = true, 则只选取100行
    val moniflow = getData(sql, originalDataPath, storePath, limitOption = true)
    val model = CFModel(moniflow,spark, sql)
    sc.stop()

  }

  //连接
  def CFModel(data:DataFrame,spark: SparkSession, sql: SQLContext)={
    /**
      * 创建映射
      */
    val indexer = new StringIndexer().setInputCol("IP").setOutputCol("IP_INDEX").fit(data)
    val dataip = indexer.transform(data)
    val indexiphost = new StringIndexer().setInputCol("HOST").setOutputCol("HOST_INDEX").fit(dataip)
    val dataiphost: DataFrame = indexiphost.transform(dataip)
    dataiphost.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //val converter = new IndexToString().setInputCol("categoryIndex").etOutputCol("originalCategory")//返回
    //打印每一列的数据类型
    dataiphost.show(20)
    dataiphost.printSchema()
    /**
      * 建模：CF协同过滤
      */
    import spark.implicits._
    val modeldata: RDD[Rating] = dataiphost.select(dataiphost("IP_INDEX").cast("Int"), dataiphost("HOST_INDEX").cast("Int"), dataiphost("COUNT").cast("Double")).map { line =>
      Rating(line.getAs[Int]("IP_INDEX"), line.getAs[Int]("HOST_INDEX"), line.getAs[Double]("COUNT"))
    }.rdd

//    计算相似度

    val columnSimilarityStorePath = "G://data//columnSim"
    val rowSimilarityStorePath = "G://data//RowSim"
    calculateSimilarity(columnSimilarityStorePath, rowSimilarityStorePath)(modeldata, spark)

    //建模计算得分
//    val rateResultSavePath = "hdfs://172.16.12.38:9000/spark/modeldong/result"
        val rateResultSavePath = "G://data//rateResult"
    calcuteRating(sql,spark, modeldata, dataiphost, rateResultSavePath)
  }

}
