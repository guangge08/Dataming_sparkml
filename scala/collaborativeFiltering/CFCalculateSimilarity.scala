package collaborativeFiltering

import java.io.File

import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, IndexedRow, MatrixEntry}
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}


/**
  * Created by Administrator on 2017/4/27 0027.
  */
object CFCalculateSimilarity {
  def calculateSimilarity(columnStorePath:String, rowStorePath:String)(modeldata: RDD[Rating], spark: SparkSession): Unit ={
    //计算列与列的相似度，返回的是RDD[Row], 每个Row是三元组 (i, j, 相似度）， i代表第i个行为，j代表第j个行为
    val simRdd : RDD[Row] = getSimilarityTupleRDD(modeldata, spark)
    //计算行与行的相似度，返回的是RDD[ROW], 每个Row是三元组 (i, j, 相似度）， i代表第i个用户的序号，j代表第j个用户的序号
    val simTransRdd: RDD[Row] = getTransSimilarityTupleRDD(modeldata)//装置文件

    //得到RDD[Row]保存到CSV
    saveCSV(columnStorePath, Array("BEHAVE1", "BEHAVE2", "SIM"), simRdd, spark)
    saveCSV(rowStorePath, Array("USER1", "USER2", "SIM"), simTransRdd, spark)

  }


  def saveCSV(savePath:String, headerName: Array[String], dataRDD: RDD[Row], spark:SparkSession): Unit ={
    val dfSchema = StructType{
      val fields = Array(StructField(headerName(0), LongType, nullable = false),
        StructField(headerName(1), LongType, nullable = false),
        StructField(headerName(2), DoubleType, nullable = false))
      fields
    }
    val df = spark.createDataFrame(dataRDD, dfSchema)
    val saveOptions = Map("header" -> "false", "delimiter" -> ",", "path" -> savePath)
    df.write.options(saveOptions).mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save()
  }


  def getSimilarityTupleRDD(tripleDF: RDD[Rating], sparkSS: SparkSession): RDD[Row] = {
    //返回（（i，j），相似度）
    //传入MatrixEntry的系数，（userIndex, productIndex, rating)
    val rddME: RDD[MatrixEntry] = {
      tripleDF.map {case rate: Rating => new MatrixEntry(rate.user.toLong, rate.product.toLong, rate.rating.toDouble) }
    }
    //传入rddME: RDD[MatrixEntry], 可以构造coordinate矩阵
    val mat: CoordinateMatrix = new CoordinateMatrix(rddME)

    val rowMat = mat.toIndexedRowMatrix()
    val m = rowMat.numRows()
    val n = rowMat.numCols()
    println(f"""一共${m} 行，${n} 列""")

    //打印UV矩阵
    val uvRows: RDD[IndexedRow] = rowMat.rows
//    printMatrix(uvRows, "U-V")

    //写入文件
    val uvRddString: RDD[String] = uvRows.map{case IndexedRow(index, vec) => {vec.toArray.foldLeft(index.toString)((acc, num) => acc + "," + num)}}

    //
    val storePath =  "G://data//UVMatrix"
    try{
      val file = new File(storePath)
      delete(file)
    }
    finally{
      uvRddString.saveAsTextFile(storePath)
    }






    //转化为rowMatrix后，计算列与列之间的相似度
    val simMatrix = rowMat.columnSimilarities()

//    //打印列相似度矩阵
//    val simRows: RDD[linalg.Vector] = simMatrix.toRowMatrix.rows
//    printSimMatrix(simRows, "Sim")

    val exactEntries = simMatrix.entries.map{ case MatrixEntry(i, j, k) => Row(i,j,k) }

    exactEntries
  }

  def getTransSimilarityTupleRDD(tripleDF: RDD[Rating]): RDD[Row] = {
    //返回（（i，j），相似度）
    //修改了传入MatrixEntry的系数，（productIndex, userIndex, rating)
    val rddME = {
      tripleDF.map {case rate: Rating => new MatrixEntry(rate.product.toLong, rate.user.toLong, rate.rating.toDouble) }
    }
    val mat: CoordinateMatrix = new CoordinateMatrix(rddME)

    //计算行的数目
    //计算列的数目
    val rowMat = mat.toRowMatrix
    val m = rowMat.numRows()
    val n = rowMat.numCols()
    println(f"""一共${m} 行，${n} 列""")



    //计算相似度矩阵
    val simMatrix: CoordinateMatrix = rowMat.columnSimilarities()
        //打印行相似度矩阵
//    val simRows: RDD[linalg.Vector] = simMatrix.toRowMatrix.rows
//    printSimMatrix(simRows, "用户相似度")



    val numRow = simMatrix.entries.count

    println(f"相似度表的行数 ${numRow}")
    val exactEntries = simMatrix.entries.map{ case MatrixEntry(i, j, k) => Row(i,j,k) }
    exactEntries
  }


//  def printMatrix(rowMat: RDD[Vector], str: String): Unit ={
//    println("打印"+ str + "矩阵")
//    rowMat.foreach(v =>println(v.toArray.foldLeft(" ")((b:String, a:Double) => (b + " " + a.toString))))
//  }


  def printMatrix(rowMat: RDD[linalg.Vector], str: String): Unit ={
    println("打印"+ str + "矩阵")
    rowMat.foreach(v =>println(v.toArray.foldLeft("")((b:String, a:Double) => (b + "\t" + a.toInt.toString))))
  }
  def printSimMatrix(rowMat: RDD[linalg.Vector], str: String): Unit ={
    println("打印"+ str + "矩阵")
    rowMat.foreach(v =>println(v.toArray.foldLeft("")((b:String, a:Double) => (b + "\t" + a.toString))))
  }
  def delete(file: File) {
    if (file.isDirectory)
      Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(delete(_))
    file.delete
  }



}
