package utils

import java.text.DecimalFormat

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by duan on 2017/5/2.
  */
trait PerformanceMeasure extends Serializable{
  //真正例 正常的被预测为正常的
  def getTP(modeltype:String)(row:Row)={
    val label=row.getAs[Double]("label");val prediction=row.getAs[Double]("prediction"+modeltype)
    (label == 1&&prediction==1)
  }
  //假正例 异常的的被预测为正常的
  def getFP(modeltype:String)(row:Row)={
    val label=row.getAs[Double]("label");val prediction=row.getAs[Double]("prediction"+modeltype)
    (label == 0&&prediction==1)
  }
  //真反例 异常的被预测为异常的
  def getTN(modeltype:String)(row:Row)={
    val label=row.getAs[Double]("label");val prediction=row.getAs[Double]("prediction"+modeltype)
    (label == 0&&prediction==0)
  }
  //假反例 正常的被预测为异常的
  def getFN(modeltype:String)(row:Row)={
    val label=row.getAs[Double]("label");val prediction=row.getAs[Double]("prediction"+modeltype)
    (label == 1&&prediction==0)
  }
  //    混淆矩阵
  val getconfusionMatrix=(testpredictionAndLabel:DataFrame,spark:SparkSession)=>{
    val TPNB=testpredictionAndLabel.filter(getTP("NB")(_)).count().toDouble;val TPLR=testpredictionAndLabel.filter(getTP("LR")(_)).count().toDouble//真正例 正常的被预测为正常的
    val FPNB=testpredictionAndLabel.filter(getFP("NB")(_)).count().toDouble;val FPLR=testpredictionAndLabel.filter(getFP("LR")(_)).count().toDouble//假正例 异常的的被预测为正常的
    val TNNB=testpredictionAndLabel.filter(getTN("NB")(_)).count().toDouble;val TNLR=testpredictionAndLabel.filter(getTN("LR")(_)).count().toDouble//真反例 异常的被预测为异常的
    val FNNB=testpredictionAndLabel.filter(getFN("NB")(_)).count().toDouble;val FNLR=testpredictionAndLabel.filter(getFN("LR")(_)).count().toDouble//假反例 正常的被预测为异常的
    val dfor=new DecimalFormat("#0.000")
    val cmDF = spark.createDataFrame(Seq(
      ("actual 1",TPNB.toInt+"||"+"precisionNB:"+dfor.format(100*TPNB/(TPNB+FPNB))+"%"+" "+"recall:"+dfor.format(100*TPNB/(TPNB+FNNB))+"%",FNNB.toInt+"||"+"precisionNB:"+dfor.format(100*FNNB/(TNNB+FNNB))+"%"+" "+"recall:"+dfor.format(100*FNNB/(TPNB+FNNB))+"%",TPLR.toInt+"||"+"precisionLR:"+dfor.format(100*TPLR/(TPLR+FPLR))+"%"+" "+"recall:"+dfor.format(100*TPLR/(TPLR+FNLR))+"%",FNLR.toInt+"||"+"precisionLR:"+dfor.format(100*FNLR/(TNLR+FNLR))+"%"+" "+"recall:"+dfor.format(100*FNLR/(TPLR+FNLR))+"%"),
      ("actual 0",FPNB.toInt+"||"+"precisionNB:"+dfor.format(100*FPNB/(TPNB+FPNB))+"%"+" "+"recall:"+dfor.format(100*FPNB/(FPNB+TNNB))+"%",TNNB.toInt+"||"+"precisionNB:"+dfor.format(100*TNNB/(TNNB+FNNB))+"%"+" "+"recall:"+dfor.format(100*TNNB/(FPNB+TNNB))+"%",FPLR.toInt+"||"+"precisionLR:"+dfor.format(100*FPLR/(TPLR+FPLR))+"%"+" "+"recall:"+dfor.format(100*FPLR/(FPLR+TNLR))+"%",TNLR.toInt+"||"+"precisionLR:"+dfor.format(100*TNLR/(TNLR+FNLR))+"%"+" "+"recall:"+dfor.format(100*TNLR/(FPLR+TNLR))+"%"))
    ).toDF("confusion matrix", "predictedNB 1","predictedNB 0","predictedLR 1","predictedLR 0")
    cmDF
  }
}
