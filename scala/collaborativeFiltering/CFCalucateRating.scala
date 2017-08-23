package collaborativeFiltering

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame,Row, SQLContext, SaveMode, SparkSession}



/**
  * Created by Administrator on 2017/4/27 0027.
  */
object CFCalucateRating {
  def calcuteRating(sql: SQLContext,spark: SparkSession, modeldata:RDD[Rating], dataiphost: DataFrame, savePath: String): Unit ={
    import spark.implicits._
    val model: MatrixFactorizationModel = ALS.train(modeldata, 50, 10, 0.1)
    //对所有用户进行推荐，每个用户推荐其前十的行为
    val topdf = recomendar(model,spark)
    //关联
    val lastdata = associaData(topdf,dataiphost,spark,sql)
    //保存结果数据
    val saveOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> savePath)
    lastdata.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save() //saveMode.Append 添加
    println("推荐算法结果保存在目录："+savePath)
    //计算训练误差//获取原始得分与模型预测得分的数据
    val userproduct: RDD[(Int, Int)] = modeldata.map { case Rating(user, product, rating) => (user, product) }
    var predictions: RDD[((Int, Int), Double)] = model.predict(userproduct).map { case Rating(user, product, rating) => ((user, product), rating) }
    val ratesAndPreds: RDD[((Int, Int), (Double, Double))] = modeldata.map { case Rating(user, product, rating) => ((user, product), rating) }.join(predictions)
    val predictedAndTrue: RDD[(Double, Double)] = ratesAndPreds.map { case ((user, product), (predicted, actual)) => (predicted, actual) }
    val regressionMetrics = new RegressionMetrics(predictedAndTrue)
    println("Mean Squared Error = " + regressionMetrics.meanSquaredError)
    println("Root Mean Squared Error = " + regressionMetrics.rootMeanSquaredError)

  }

  def recomendar(model: MatrixFactorizationModel,spark:SparkSession)={
    import spark.implicits._
    val allusertopK: RDD[(Int, Array[Rating])] = model.recommendProductsForUsers(10)
    allusertopK.take(30).foreach { row => println(row._2.mkString("\t")) }
    val topcontact: RDD[Rating] = allusertopK.flatMap(row => row._2)
    //将所有Rating首尾拼起来
    val topdf = topcontact.map { row => (row.user, row.product, row.rating) }.toDF("IP_INDEX", "HOST_INDEX", "SCORES")
    println("转化的得分df：")
    topdf.show(20)
    topdf
  }

  def associaData(topdf:DataFrame,dataiphost: DataFrame,spark:SparkSession,sql:SQLContext)={
    import spark.implicits._
    val dataiphost2 = dataiphost.select(dataiphost("IP").cast("String"), dataiphost("HOST").cast("String"), dataiphost("IP_INDEX").cast("Int"), dataiphost("HOST_INDEX").cast("Int"), dataiphost("COUNT").cast("Double")).toDF.map { line =>
      (line.getAs[String]("IP"), line.getAs[String]("HOST"), line.getAs[Int]("IP_INDEX"), line.getAs[Int]("HOST_INDEX"), line.getAs[Double]("COUNT"))
    }.toDF("IP", "HOST", "IP_INDEX", "HOST_INDEX", "COUNT")
    topdf.createOrReplaceTempView("topdf")
    dataiphost2.createOrReplaceTempView("dataiphost")
    val lastdata: DataFrame = spark.sql(
      s"select distinct a.IP_INDEX,a.HOST_INDEX,a.SCORES,a.IP,b.HOST from (select t.IP_INDEX,t.HOST_INDEX,t.SCORES,d.IP from topdf t left join dataiphost d on t.IP_INDEX=d.IP_INDEX) a left join dataiphost b on a.HOST_INDEX = b.HOST_INDEX"
    )
    sql.dropTempTable("topdf")
    sql.dropTempTable("dataiphost")
    println("最终df：")
    lastdata.show(50)
    lastdata
  }

  def recomendarSingle(model: MatrixFactorizationModel,k:Int,item:Int,dataiphost:DataFrame)={
    //特定用户生成前k个推荐物
    val topK: Array[Rating] = model.recommendProducts(k,item)
    println("给资产"+k.toString+"推荐的行为：")
    println(topK.mkString("\n"))
    println(k.toString+"资产原本的行为：")
    dataiphost.filter(s"IP_INDEX='$k+.0'").show(10)
  }

  def getUserIpHost(datardd:DataFrame,dataiphost:DataFrame,spark:SparkSession,sql:SQLContext){
    import spark.implicits._
    val improtdata: DataFrame =datardd.map{ case Row(i,j,k) => (i.toString+".0",j.toString+".0",k)}.toDF("IP1","IP2","SIM")
    improtdata.createOrReplaceTempView("importdata")
    dataiphost.createOrReplaceTempView("dataiphost")
    spark.sql(s"select d.IP IP_LEFT,t.IP1,t.IP2,t.SIM from importdata t left join dataiphost d on t.IP1=d.IP_INDEX").createOrReplaceTempView("dataip")
    val lastdata = spark.sql(s"select t.IP_LEFT,d.IP IP_RIGHT,t.IP1,t.IP2,t.SIM from dataip t left join dataiphost d on t.IP2=d.IP_INDEX")
    lastdata.show(false)
    sql.dropTempTable("importdata")
    sql.dropTempTable("dataiphost")
    sql.dropTempTable("dataip")
    lastdata.select("IP_LEFT","IP_RIGHT","SIM").toDF
  }
  def getHostIpHost(datardd:DataFrame,dataiphost:DataFrame,spark:SparkSession,sql:SQLContext){
    import spark.implicits._
    val improtdata: DataFrame =datardd.map{ case Row(i,j,k) => (i.toString+".0",j.toString+".0",k)}.toDF("HOST1","HOST2","SIM")
    improtdata.createOrReplaceTempView("importdata")
    dataiphost.createOrReplaceTempView("dataiphost")
    spark.sql(s"select d.HOST HOST_LEFT,t.HOST1,t.HOST2,t.SIM from importdata t left join dataiphost d on t.HOST1=d.HOST_INDEX").createOrReplaceTempView("dataip")
    val lastdata = spark.sql(s"select t.HOST_LEFT,d.HOST HOST_RIGHT,t.HOST1,t.HOST2,t.SIM from dataip t left join dataiphost d on t.HOST2=d.HOST_INDEX")
    lastdata.show(false)
    sql.dropTempTable("importdata")
    sql.dropTempTable("dataiphost")
    sql.dropTempTable("dataip")
    lastdata.select("HOST_LEFT","HOST_RIGHT","SIM").toDF
  }

}
