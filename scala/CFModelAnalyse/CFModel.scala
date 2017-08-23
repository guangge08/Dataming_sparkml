package CFModelAnalyse

import java.io.File
import java.io._
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties, UUID}

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import utils.ROWUtils

import scala.collection.immutable
import scala.collection.parallel.immutable.ParSeq
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._


/**
  * Created by huoguang on 2017/5/5.
  */
class CFModel extends Serializable with dataAccess {

  //协同过滤主函数
  def CFModel(data: (DataFrame, DataFrame), cltime:Long, properties: Properties, spark: SparkSession, sql: SQLContext) = {
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>协同过滤 start time:" + getNowTime() + ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    //输入的data的列为：IP,HOST,TIME,COUNT
    //创建映射
    val dataiphost: DataFrame = indexData(data._1,spark)
    val dataiphostnotime: DataFrame = indexData(data._2,spark,false)
    //建模
    //CF 协同过滤
    val time= List("morning", "noon", "afternoon", "night", "midnight")
    val selectData: List[(String, DataFrame)] = time.map(eachtime => (eachtime, dataiphost.filter(s"TIME='$eachtime'").toDF()))
    val unionFun = (a:DataFrame, b:DataFrame) =>a.union(b).toDF
    //推荐模型
    val datatTemp: List[DataFrame] = selectData.map(pair => collProcess(pair._2, dataiphost, pair._1, spark, sql,properties))
    val uniondata = datatTemp.tail.foldRight(datatTemp.head)(unionFun)
    val colldata = normalizationColl(uniondata,cltime,spark)
    //计算行为之间的相似性
    var productSimResult = productSim(dataiphostnotime, cltime, spark)

    //计算用户相似性去掉时间标签
    var userSimTemp = userSim(dataiphostnotime, spark)
    val userSimResult = normalizationSim(userSimTemp,cltime,spark,false)
    val userSimreason = userSimReason(userSimTemp,data._2,cltime,spark,sql,false)

    //计算用户之间的相似性(带有时间标签)
//    val userSimTemp = selectData.map(pair => userSim(pair._2, pair._1,spark))
//    val userSimTemp2 = userSimTemp.tail.foldRight(userSimTemp.head)(unionFun)
//    val userSimreason = userSimReason(userSimTemp2,data,spark,sql)
//    val userSimResult = normalizationSim(userSimTemp2,spark)

    //保存结果到HDFS
    val collaborativeResultPath: Array[String] = Array("coll_result", "usersim_result","usersim_reason","productsim_result").map(properties.getProperty(_))//修改
    val Array(rateResultSavePath, rowSimilarityStorePath, userSimreasonPath,columnSimilarityStorePath) = collaborativeResultPath//修改,分别赋值
    saveDF(colldata, rateResultSavePath)
    saveDF(userSimResult, rowSimilarityStorePath)
    saveDF(userSimreason, userSimreasonPath)
    saveDF(productSimResult, columnSimilarityStorePath)
    //保存结果到ES
    val esResultPath: Array[String] = Array("es.save.cf.stats", "es.save.sim","es.save.sim.reason","es.save.sim.v")//.map(properties.getProperty(_))//修改
    val Array(rateESPath, rowSimilarityESPath, userSimreasonESPath,columnSimilarityESPath) = esResultPath//修改,分别赋值
    saveToES(colldata,rateESPath,properties,sql)
    saveToES(userSimResult,rowSimilarityESPath,properties,sql)
    saveToES(userSimreason,userSimreasonESPath,properties,sql)
    saveToES(productSimResult,columnSimilarityESPath,properties,sql)
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>协同过滤 end time:" + getNowTime() + ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
  }

  def userSimReason(usersimdata: DataFrame, stat:DataFrame, time:Long, spark: SparkSession, sql: SQLContext,usetime:Boolean=true)={
    //usersim:IP1,IP2,SIM
    //statdata:IP,HOST,TIME,COUNT
    usersimdata.createOrReplaceTempView("usersim")
    stat.createOrReplaceTempView("statdata")
//    statdata.createOrReplaceTempView("statdata")
    //IP IP IP1,b.IP IP2,a.SIM,a.TIME
    //IP", "HOST", "TIME", "COUNT
    //sql join
//    println("ueseim")
//    usersimdata.show(false)
//    println("statDATA")
//    stat.show(false)
    //时间标签
    var time1 = ""
    var time2 =""
    if(usetime){
      time1 = ",t.TIME"
      time2 = ",TIME"
    }
    val str ="select t.IP1,t.IP2,t.SIM"+time1+",s.IP,s.HOST IP1HOST ,s.COUNT IP1COUNT from usersim t left join statDATA s on t.IP1=s.IP"
    val temp1 = spark.sql(str)
    temp1.createOrReplaceTempView("step1")
//    println("此处是第一次关联:IP1HOST,IP1COUNT")
//    temp1.show(10,false)
    val str2 = "select t.*,s.IP IP22,s.HOST IP2HOST,s.COUNT IP2COUNT from step1 t left join statDATA s on t.IP2=s.IP"
    val temp2 = spark.sql(str2)
    temp2.createOrReplaceTempView("step2")
//    println("此处是第二次关联：IP2HOST,IP2COUNT")
//    temp2.show(10,false)
    val str3 = "select IP1,IP2,IP1HOST,IP2HOST"+time2+",CAST(SIM AS double),CAST(IP1COUNT AS int),CAST(IP2COUNT AS int),IP1COUNT/IP2COUNT RATE from step2 where IP1HOST=IP2HOST order by IP1,IP2,RATE"//
    val statdata = spark.sql(str3)
    import spark.implicits._
    val result = statdata.rdd.map{line=>
      val ip1 = line.getAs[String]("IP1")
      val ip2 = line.getAs[String]("IP2")
      val ip1host = line.getAs[String]("IP1HOST")
      val ip2host = line.getAs[String]("IP2HOST")
      val sim = line.getAs[Double]("SIM").toFloat
      val ip1count = line.getAs[Int]("IP1COUNT")
      val ip2count = line.getAs[Int]("IP2COUNT")
      val rate = line.getAs[Double]("RATE").toFloat
      val recordid: String = ROWUtils.genaralROW()
      val cltime = time
      (recordid,ip1,ip2,ip1host,ip2host,sim,ip1count,ip2count,rate,cltime)
    }.toDF("row","ip","other_ip","host","other_host","sim","ip_count","other_ip_count","rate","cltime")
    //.toDF("ROW","IP","OTHER_IP","HOST","OTHER_HOST","SIM","IP_COUNT","OTHER_IP_COUNT","RATE","CLTIME")
//    println("此处是第三次关联:filter the IP1HOST and IP2HOST")
    println("the similarity of users:")
    result.show(5,false)
    result
  }


  //对建模前的数据进行INDEX映射
  def indexData(data:DataFrame,spark:SparkSession,usetime:Boolean=true)={
    val indexer = new StringIndexer().setInputCol("IP").setOutputCol("IP_INDEX").fit(data)
    val dataip = indexer.transform(data)
    val indexiphost = new StringIndexer().setInputCol("HOST").setOutputCol("HOST_INDEX").fit(dataip)
    val dataih: DataFrame = indexiphost.transform(dataip)

    import spark.implicits._
    var dataiphost:DataFrame = null
    //含有时间标签的数据
    if(usetime){
      dataiphost = dataih.rdd.map { row =>
        (row.getAs[String]("IP"), row.getAs[String]("HOST"), row.getAs[String]("TIME"), row.getAs[Double]("COUNT"), row.getAs[Double]("IP_INDEX").toInt, row.getAs[Double]("HOST_INDEX").toInt)
      }.toDF("IP", "HOST", "TIME", "COUNT", "IP_INDEX", "HOST_INDEX")
    }
    //不含有时间标签的数据
    else{
      dataiphost = dataih.rdd.map { row =>
        (row.getAs[String]("IP"), row.getAs[String]("HOST"), row.getAs[Double]("COUNT"), row.getAs[Double]("IP_INDEX").toInt, row.getAs[Double]("HOST_INDEX").toInt)
      }.toDF("IP", "HOST", "COUNT", "IP_INDEX", "HOST_INDEX")
    }
    dataiphost.persist(StorageLevel.MEMORY_AND_DISK_SER)
    println("完成：协同过滤需要的数据")
    dataiphost
  }

  //协同过滤中间步
  def collProcess(selectdata: DataFrame, dataiphost: DataFrame, eachtime: String, spark: SparkSession, sql: SQLContext,properties: Properties) = {
    import spark.implicits._
    val modeldata = selectdata.select(selectdata("IP_INDEX").cast("Int"), selectdata("HOST_INDEX").cast("Int"), selectdata("COUNT").cast("Double")).rdd.map { line =>
      Rating(line.getAs[Int]("IP_INDEX"), line.getAs[Int]("HOST_INDEX"), line.getAs[Double]("COUNT"))
    }
    //建立推荐模型以及进行推荐
    var collresult = calcuteRating(sql, spark, modeldata, dataiphost,properties)
//    val collresult = calcuteRatingIter(sql, spark, modeldata, dataiphost,eachtime)//调参使用
    //将结果转化成DF
    val conbinedata = collresult.map { row =>
      (row.getAs[String]("IP"), row.getAs[String]("HOST"), row.getAs[Double]("SCORES").formatted("%.6f"), eachtime)//row.getAs[Int]("IP_INDEX"), row.getAs[Int]("HOST_INDEX")
    }.toDF("IP", "HOST", "SCORES", "TIME")//"IP_INDEX", "HOST_INDEX"
    conbinedata
  }

  //协同过滤模型
  def calcuteRating(sql: SQLContext, spark: SparkSession, modeldata: RDD[Rating], dataiphost: DataFrame,properties: Properties) = {
  //读取配置文件
    val List(rank, iterations, blocks, recommendNum) = List("rank", "iterations", "blocks","recommend.num").map(s => properties.getProperty("CF." + s).toInt)
    val List(lambda, alpha) = List("lambda", "alpha").map(s => properties.getProperty("CF." + s).toDouble)//修改
//    val model = ALS.trainImplicit(modeldata, 50, 10, 0.01, 0.01)
    val model = ALS.trainImplicit(ratings=modeldata,rank=rank,iterations=iterations,lambda=lambda,blocks=blocks,alpha=alpha)
    //对所有用户进行推荐，每个用户推荐其前十的行为
//    val topdf = recomendar(model, spark)
    import spark.implicits._
    val allusertopK: RDD[(Int, Array[Rating])] = model.recommendProductsForUsers(recommendNum)
    //格式转换：将所有Rating首尾拼起来
    val topcontact: RDD[Rating] = allusertopK.flatMap(row => row._2)
    val topdf = topcontact.map(row => (row.user, row.product, row.rating)).toDF("IP_INDEX", "HOST_INDEX", "SCORES")
    //关联数据（关联原始IP和HOST）
    val lastdata = associaData(topdf, dataiphost, spark, sql)
    //计算训练误差//获取原始得分与模型预测得分的数据
    val userproduct = modeldata.map { case Rating(user, product, rating) => (user, product) }
    var predictions = model.predict(userproduct).map { case Rating(user, product, rating) => ((user, product), rating) }
    val ratesAndPreds = modeldata.map { case Rating(user, product, rating) => ((user, product), rating) }.join(predictions)
    val predictedAndTrue = ratesAndPreds.map { case ((user, product), (predicted, actual)) => (predicted, actual) }
    val regressionMetrics = new RegressionMetrics(predictedAndTrue)
    println("完成：ALS模型的误差计算")
    println("Mean Squared Error = " + regressionMetrics.meanSquaredError)
    println("Root Mean Squared Error = " + regressionMetrics.rootMeanSquaredError)
    lastdata
  }

  //只供CF模型参数调优使用
  def calcuteRatingIter(sql: SQLContext, spark: SparkSession, modeldata: RDD[Rating], dataiphost: DataFrame,eachtime: String) = {
    //将所有记录写进文件
    val filePath = System.getProperty("user.dir")
    val writer = new PrintWriter(new File(filePath + "/Squared_Error.txt"))
    writer.write("time\trank\tblocks\tlambda\talpha\titerations\tSquared_Error\n")
    for(eachrank <- Range(100,300,10); eachblocks <- Range(10, 11); eachlambda <- Range(1,50,3).map(_*0.01); eachalpha <- Range(1,50,3).map(_*0.01)) {
      println("The params:  >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"+eachtime)
      println("rank:" + eachrank + "  blocks:" + eachblocks + " lambda:" + eachlambda + " alpha:" + eachalpha)
      val model = ALS.trainImplicit(ratings = modeldata, rank = eachrank, iterations = 10, lambda = eachlambda, blocks = eachblocks, alpha = eachalpha)
      //误差计算
      val userproduct: RDD[(Int, Int)] = modeldata.map { case Rating(user, product, rating) => (user, product) }
      val predictions: RDD[((Int, Int), Double)] = model.predict(userproduct).map { case Rating(user, product, rating) => ((user, product), rating) }
      val ratesAndPreds: RDD[((Int, Int), (Double, Double))] = modeldata.map { case Rating(user, product, rating) => ((user, product), rating) }.join(predictions)
      val predictedAndTrue: RDD[(Double, Double)] = ratesAndPreds.map { case ((user, product), (predicted, actual)) => (predicted, actual) }
      val regressionMetrics = new  RegressionMetrics(predictedAndTrue)
      println("Mean Squared Error = " + regressionMetrics.meanSquaredError)
      writer.write(eachtime+"\t"+ eachrank + "\t" + eachblocks + "\t" + eachlambda + "\t" + eachalpha + "\t" + 10 + "\t" + regressionMetrics.meanSquaredError + "\n")
      model
    }//修改
    writer.close()
    val model1 = ALS.trainImplicit(modeldata, 50, 10, 0.01, 0.01)
    val topdf = recomendar(model1, spark)
    //关联
    val lastdata = associaData(topdf, dataiphost, spark, sql)
    lastdata
  }

  def recomendar(model: MatrixFactorizationModel, spark: SparkSession) = {
    import spark.implicits._
    val allusertopK: RDD[(Int, Array[Rating])] = model.recommendProductsForUsers(10)
    //将所有Rating首尾拼起来
    val topcontact: RDD[Rating] = allusertopK.flatMap(row => row._2)
    val topdf = topcontact.map(row => (row.user, row.product, row.rating)).toDF("IP_INDEX", "HOST_INDEX", "SCORES")
    topdf
  }

  def associaData(topdf: DataFrame, dataiphost: DataFrame, spark: SparkSession, sql: SQLContext) = {
    import spark.implicits._
    val dataiphost2 = dataiphost.select(dataiphost("IP").cast("String"), dataiphost("HOST").cast("String"), dataiphost("IP_INDEX").cast("Int"), dataiphost("HOST_INDEX").cast("Int"), dataiphost("COUNT").cast("Double")).toDF.map { line =>
      (line.getAs[String]("IP"), line.getAs[String]("HOST"), line.getAs[Int]("IP_INDEX"), line.getAs[Int]("HOST_INDEX"), line.getAs[Double]("COUNT"))
    }.toDF("IP", "HOST", "IP_INDEX", "HOST_INDEX", "COUNT")
    topdf.createOrReplaceTempView("topdf")
    dataiphost2.createOrReplaceTempView("dataiphost")
    val lastdata = spark.sql(s"select distinct a.IP_INDEX,a.HOST_INDEX,a.SCORES,a.IP,b.HOST from (select t.IP_INDEX,t.HOST_INDEX,t.SCORES,d.IP from topdf t left join dataiphost d on t.IP_INDEX=d.IP_INDEX) a left join dataiphost b on a.HOST_INDEX = b.HOST_INDEX")
//    sql.dropTempTable("topdf")
//    sql.dropTempTable("dataiphost")
    Seq("topdf", "dataiphost").map(sql.dropTempTable(_))
    lastdata
  }

  def recomendarSingle(model: MatrixFactorizationModel, k: Int, item: Int, dataiphost: DataFrame) = {
    //特定用户生成前k个推荐物
    val topK: Array[Rating] = model.recommendProducts(k, item)
    println("给资产" + k.toString + "推荐的行为：")
    println(topK.mkString("\n"))
    println(k.toString + "资产原本的行为：")
    dataiphost.filter(s"IP_INDEX='$k+.0'")
  }

  //将推荐结果进行入库数据格式化
  def normalizationColl(dataiphost:DataFrame,time:Long,spark:SparkSession)={
    val max = dataiphost.select("SCORES").rdd.map(_(0).toString.toDouble).max
    val min: Double = 0.0
    import spark.implicits._
    val result: DataFrame = dataiphost.rdd.map{ row =>
      val scl = (row.getAs[String]("SCORES").toDouble-min)/(max-min)
      val recordid: String = ROWUtils.genaralROW()
      val datatype: String = "CF"
      val cltime = time
      (recordid,row.getAs[String]("IP"),row.getAs[String]("HOST"),scl.toFloat,row.getAs[String]("TIME"),datatype,cltime)
    }.toDF("row","ip","behav","scores","time","data_type","cltime")
    //.toDF("ROW","IP","BEHAV","SCORES","TIME","DATA_TYPE","CLTIME")
    result
  }

  def normalizationSim(dataiphost:DataFrame,time:Long,spark:SparkSession,usetime:Boolean=true)={
    import spark.implicits._
    var result:DataFrame = null
    if(usetime){
      result = dataiphost.rdd.map{ row =>
        val recordid: String = ROWUtils.genaralROW()
        val datatype: String = "U"
        val cltime = time
        (recordid,row.getAs[String]("IP1"),row.getAs[String]("IP2"),row.getAs[String]("SIM").toFloat,row.getAs[String]("TIME"),datatype,cltime)
      }.toDF("row","object","other_object","sim","time","data_type","cltime")//.toDF("ROW","OBJECT","OTHER_OBJECT","SIM","TIME","DATA_TYPE","CLTIME")
    }
    else{
      result = dataiphost.rdd.map{ row =>
        val recordid: String = ROWUtils.genaralROW()
        val datatype: String = "V"
        val cltime = time
        (recordid,row.getAs[String]("IP1"),row.getAs[String]("IP2"),row.getAs[String]("SIM").toFloat,datatype,cltime)
      }.toDF("row","object","other_object","sim","data_type","cltime")//.toDF("ROW","OBJECT","OTHER_OBJECT","SIM","DATA_TYPE","CLTIME")
    }
    result
  }


  //计算用户相似性矩阵//考虑合并函数？？？
  def userSim(modeldata: DataFrame, spark: SparkSession) = {
    val modeldata1 = modeldata.rdd.map(row => Rating(row.getAs[Int]("IP_INDEX"), row.getAs[Int]("HOST_INDEX"), row.getAs[Double]("COUNT")))
    val simResult = getTransSimilarityTupleRDD(modeldata1)
    import spark.implicits._
    val userSim = simResult.map(row => (row._1, row._2, row._3.formatted("%.3f"))).toDF("USER1", "USER2", "SIM")
    //关联获取IP和HOST
    modeldata.createOrReplaceTempView("modeldata")
    userSim.createOrReplaceTempView("userSim")
    //加上时间标签
    //val userSimResult: DataFrame = spark.sql(s"select distinct a.IP IP1,b.IP IP2,a.SIM,a.TIME from (select t.USER1,t.USER2,t.SIM,d.TIME,d.IP,d.IP_INDEX from userSim t left join modeldata d on t.USER1=d.IP_INDEX) a left join modeldata b on a.USER2 = b.IP_INDEX")
    //没有时间标签
    val userSimResult: DataFrame = spark.sql(s"select distinct a.IP IP1,b.IP IP2,a.SIM from (select t.USER1,t.USER2,t.SIM,d.IP,d.IP_INDEX from userSim t left join modeldata d on t.USER1=d.IP_INDEX) a left join modeldata b on a.USER2 = b.IP_INDEX")
    userSimResult
  }

//  def calculateSimilarity(modeldata: RDD[Rating], spark: SparkSession) = {
//    //计算行与行的相似度，返回的是RDD[ROW], 每个Row是三元组 (i, j, 相似度）， i代表第i个用户的序号，j代表第j个用户的序号
//    val simTransRdd = getTransSimilarityTupleRDD(modeldata) //装置文件
//    simTransRdd
//  }

  def getTransSimilarityTupleRDD(tripleDF: RDD[Rating]) = {
    //返回（（i，j），相似度）
    //修改了传入MatrixEntry的系数，（productIndex, userIndex, rating)
    val rddME = {
      tripleDF.map { case rate: Rating => new MatrixEntry(rate.product.toLong, rate.user.toLong, rate.rating.toDouble) }
    }
    val mat: CoordinateMatrix = new CoordinateMatrix(rddME)
    //计算行的数目、计算列的数目
    val rowMat = mat.toRowMatrix
//    val m = rowMat.numRows()
//    val n = rowMat.numCols()
    //计算相似度矩阵
    val simMatrix: CoordinateMatrix = rowMat.columnSimilarities()
    val numRow = simMatrix.entries.count
    val exactEntries: RDD[(Long, Long, Double)] = simMatrix.entries.map { case MatrixEntry(i, j, k) => (i, j, k) }
    println("完成：计算U-U相似矩阵")
    exactEntries
  }

  def getNowTime(): String = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time = new Date().getTime
    format.format(time)
  }

  //保存到CSV
  def saveCSV(savePath: String, headerName: Array[String], dataRDD: RDD[Row], spark: SparkSession): Unit = {
    val dfSchema = StructType {
      val fields = Array(StructField(headerName(0), LongType, nullable = false),
        StructField(headerName(1), LongType, nullable = false),
        StructField(headerName(2), DoubleType, nullable = false))
      fields
    }
    val df = spark.createDataFrame(dataRDD, dfSchema)
    val saveOptions = Map("header" -> "false", "delimiter" -> ",", "path" -> savePath)
    df.write.options(saveOptions).mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save()
  }


  //计算行为相似性矩阵
    def productSim(modeldata: DataFrame, time:Long, spark: SparkSession) = {
      val modeldata1 = modeldata.rdd.map(row => Rating(row.getAs[Int]("IP_INDEX"), row.getAs[Int]("HOST_INDEX"), row.getAs[Double]("COUNT")))
  //    val CFCalculateSimilarity = new CFCalculateSimilarity()
      val simResult = getSimilarityTupleRDD(modeldata1)
      import spark.implicits._
      val productSim = simResult.map(row => (row._1, row._2, row._3)).toDF("BEHAVIOR1", "BEHAVIOR2", "SIM")
      //关联获取HOST
      modeldata.createOrReplaceTempView("modeldata2")
      productSim.createOrReplaceTempView("productSim")
      val productSimResult: DataFrame = spark.sql(s"select distinct a.HOST HOST1,b.HOST HOST2,CAST(a.SIM AS double) from (select t.BEHAVIOR1,t.BEHAVIOR2,t.SIM,d.HOST,d.HOST_INDEX from productSim t left join modeldata2 d on t.BEHAVIOR1=d.HOST_INDEX) a left join modeldata2 b on a.BEHAVIOR2 = b.HOST_INDEX where a.SIM >=0.7")
      //标准化入库数据格式
      import spark.implicits._
      val result = productSimResult.rdd.map{line=>
        val host1 = line.getAs[String]("HOST1")
        val host2 = line.getAs[String]("HOST2")
        val sim = line.getAs[Double]("SIM").toFloat
        val recordid: String = ROWUtils.genaralROW()
        val cltime = time
        val data_type = "V"
        (recordid,host1,host2,sim,cltime,data_type)
      }.toDF("row","object","other_object","sim","cltime","data_type")
      //.toDF("ROW","OBJECT","OTHER_OBJECT","SIM","CLTIME","DATA_TYPE")
      result
    }

//    def calculateSimilarity(modeldata: RDD[Rating], spark: SparkSession) = {
//      //计算列与列的相似度，返回的是RDD[Row], 每个Row是三元组 (i, j, 相似度）， i代表第i个行为，j代表第j个行为
//      val simRdd = getSimilarityTupleRDD(modeldata)
//      //计算行与行的相似度，返回的是RDD[ROW], 每个Row是三元组 (i, j, 相似度）， i代表第i个用户的序号，j代表第j个用户的序号
//      val simTransRdd = getTransSimilarityTupleRDD(modeldata) //装置文件
//      (simRdd, simTransRdd)
//    }

    def getSimilarityTupleRDD(tripleDF: RDD[Rating]) = {
      //返回（（i，j），相似度）
      //传入MatrixEntry的系数，（userIndex, productIndex, rating)
      val rddME: RDD[MatrixEntry] = {
        tripleDF.map { case rate: Rating => new MatrixEntry(rate.user.toLong, rate.product.toLong, rate.rating.toDouble) }
      }
      //传入rddME: RDD[MatrixEntry], 可以构造coordinate矩阵
      val mat: CoordinateMatrix = new CoordinateMatrix(rddME)
      val rowMat = mat.toIndexedRowMatrix()
      val m = rowMat.numRows()
      val n = rowMat.numCols()
//      println("m:"+m)
//      println("n:"+n)
      //转化为rowMatrix后，计算列与列之间的相似度
      val simMatrix = rowMat.columnSimilarities()
      val exactEntries = simMatrix.entries.map { case MatrixEntry(i, j, k) => (i, j, k) }
      println("完成：计算V-V相似矩阵")
      exactEntries
    }

  //保存数据到HDFS
  def saveDF(data: DataFrame, path: String, reparttition: Boolean = true) = {
    val saveOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> path)
    if (reparttition) {
      data.repartition(1).write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
    } //saveMode.Append 添加
    else {
      data.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
    }
    println("complete: save to HDFS,on " + path)
  }

  //保存数据到ES
//  def saveToES(data:DataFrame,sql: SQLContext,indexs:String)= {
//    try {
//      data.saveToEs(indexs)
//      println("complete:save to ES,on "+indexs)
//    } catch {
//      case e: Exception => println("error on save to ES：" + e.getMessage)
//    } finally {}
//  }



//  def getSimilarityTupleRDD(tripleDF: RDD[Rating]) = {
//    //返回（（i，j），相似度）
//    //传入MatrixEntry的系数，（userIndex, productIndex, rating)
//    tripleDF.take(10).foreach(println)
//    val rddME: RDD[MatrixEntry] = {
//      tripleDF.map { case rate: Rating => new MatrixEntry(rate.user.toLong, rate.product.toLong, rate.rating.toDouble) }
//    }
//    //传入rddME: RDD[MatrixEntry], 可以构造coordinate矩阵
//    val mat: CoordinateMatrix = new CoordinateMatrix(rddME)
//    val rowMat: CoordinateMatrix = mat.toRowMatrix().columnSimilarities()
//    //转化为rowMatrix后，计算列与列之间的相似度
//    val exactEntries = rowMat.entries.map { case MatrixEntry(i, j, k) => (i, j, k) }
//    println("完成：计算V-V相似矩阵")
//    exactEntries
//  }



  //  def getUserIpHost(datardd: DataFrame, dataiphost: DataFrame, spark: SparkSession, sql: SQLContext) {
  //    import spark.implicits._
  //    val improtdata: DataFrame = datardd.map { case Row(i, j, k) => (i.toString + ".0", j.toString + ".0", k) }.toDF("IP1", "IP2", "SIM")
  //    improtdata.createOrReplaceTempView("importdata")
  //    dataiphost.createOrReplaceTempView("dataiphost")
  //    spark.sql(s"select d.IP IP_LEFT,t.IP1,t.IP2,t.SIM from importdata t left join dataiphost d on t.IP1=d.IP_INDEX").createOrReplaceTempView("dataip")
  //    val lastdata = spark.sql(s"select t.IP_LEFT,d.IP IP_RIGHT,t.IP1,t.IP2,t.SIM from dataip t left join dataiphost d on t.IP2=d.IP_INDEX")
  //    //    lastdata.show(false)
  //    Seq("importdata", "dataiphost", "dataip").map(sql.dropTempTable(_))
  //    lastdata.select("IP_LEFT", "IP_RIGHT", "SIM").toDF
  //  }
  //
  //  def getHostIpHost(datardd: DataFrame, dataiphost: DataFrame, spark: SparkSession, sql: SQLContext) {
  //    import spark.implicits._
  //    val improtdata: DataFrame = datardd.map { case Row(i, j, k) => (i.toString + ".0", j.toString + ".0", k) }.toDF("HOST1", "HOST2", "SIM")
  //    improtdata.createOrReplaceTempView("importdata")
  //    dataiphost.createOrReplaceTempView("dataiphost")
  //    spark.sql(s"select d.HOST HOST_LEFT,t.HOST1,t.HOST2,t.SIM from importdata t left join dataiphost d on t.HOST1=d.HOST_INDEX").createOrReplaceTempView("dataip")
  //    val lastdata = spark.sql(s"select t.HOST_LEFT,d.HOST HOST_RIGHT,t.HOST1,t.HOST2,t.SIM from dataip t left join dataiphost d on t.HOST2=d.HOST_INDEX")
  //    //    lastdata.show(false)
  //    Seq("importdata", "dataiphost", "dataip").map(sql.dropTempTable(_))
  //    lastdata.select("HOST_LEFT", "HOST_RIGHT", "SIM").toDF
  //  }

}