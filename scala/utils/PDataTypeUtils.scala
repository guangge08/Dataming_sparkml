package utils

import java.sql.Timestamp

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.phoenix.schema.types._
import org.apache.spark.sql
import org.apache.spark.sql.types._

import scala.util.Try

/**
  * Created by duan on 2017/4/13.
  */
trait PDataTypeUtils extends ConfSupport{

  //得到统一为String的值
  def getStringValue(hrow: (ImmutableBytesWritable, Result), colfamily: String) = {
    val util = (colconfig: ColConfig) => {
      val dataBYTES = if (colconfig.colname != "ROW") hrow._2.getValue(Bytes.toBytes(colfamily), Bytes.toBytes(colconfig.colname)) else hrow._2.getRow
      if (dataBYTES != null) PDataType.fromTypeId(colconfig.coltype).toObject(dataBYTES).toString else "-"
    }
    util
  }

  //得到各自类型的值
  def getValue(hrow: (ImmutableBytesWritable, Result), colfamily: String,style:Int) = {
    val util = (colconfig: ColConfig) => {
      val dataBYTES = if (colconfig.colname != "ROW") hrow._2.getValue(Bytes.toBytes(colfamily), Bytes.toBytes(colconfig.colname)) else hrow._2.getRow
      val strResult = Try(PDataType.fromTypeId(colconfig.coltype).toObject(dataBYTES).toString).getOrElse(null)
      (colconfig.coltype,style) match {
        case (_,0)=>strResult
        case (12,1)=>strResult
        case (4,1)=>Try(strResult.toDouble.toInt).getOrElse(null)
        case (8,1)=>Try(strResult.toDouble).getOrElse(null)
        case (6,1)=>Try(strResult.toFloat).getOrElse(null)
        case (93,1)=>Try(Timestamp.valueOf(strResult)).getOrElse(null)
        case (-5,1)=>Try(strResult.toDouble.toLong).getOrElse(null)
      }
    }
    util
  }

  //获取数据结构
  def getschema(colconfigs: List[ColConfig],style:Int)={
    StructType.apply(for(col<-colconfigs)yield StructField(col.colname,NumToType(col.coltype,style)))
  }
  //获取字段配置：字段名和字段的数字类型
  def getColConfigs(tablename:String,colnames:List[String]) ={
    for(col<-colnames)yield ColConfig(col,getconf(tablename + "." + col).trim.toInt)
  }

  //获取一行所有字段的值
  def getColValues(hrow: (ImmutableBytesWritable, Result),colfamily:String,configs: List[ColConfig],style:Int)={
    val util= getValue(hrow,colfamily,style)
    for(colconfig<-configs)yield util(colconfig)
  }

  //字段的数字类型转换成数据类型
  def NumToType(coltype:Int,style:Int)={
    (coltype,style) match {
      case (_,0)=>StringType
      case (12,1)=>StringType
      case (4,1)=>IntegerType
      case (8,1)=>DoubleType
      case (6,1)=>FloatType
      case (93,1)=>TimestampType
      case (-5,1)=>LongType
    }
  }




  //保存到Hbase
  def getSavePut(line: sql.Row, configs: List[ColConfig], putTableName: String, colfamily: String) = {
    var put = new Put(Bytes.toBytes(Try(line.getAs[String]("ROW")).getOrElse(ROWUtils.genaralROW())))
    for (colconfig <- configs) {
      val colname=colconfig.colname
      val rawvalue=Try(line.getAs[String](colname).trim).getOrElse(null)
      colname match {
        case "ROW" => 0
        case _ => {
          if(rawvalue!=""&&rawvalue!="-1") {
            val coltype = colconfig.coltype
            val getBytes=(value:Any)=>{PDataType.fromTypeId(coltype).toBytes(value)}
            val colvalue = Try(coltype match {
              case 12 => rawvalue
              case 4 => rawvalue.toInt
              case 8 => rawvalue.toDouble
              case 6 => rawvalue.toFloat
              case 93 => Timestamp.valueOf(rawvalue)
              case -5 => rawvalue.toLong
            }).getOrElse(null)
            if(colvalue!=null) {
              val colBytes = getBytes(colvalue)
              put.addColumn(Bytes.toBytes(colfamily), Bytes.toBytes(colname), colBytes)
            }
          }
        }
      }
    }
    put
  }
}


//def saveToHBase(put: Put, colfamily: String, line: sql.Row) = {
//  val utils = (colconfig: ColConfig) => {
//  println(">>>>>>>>>>>>>>>>>>>>>>>>>>进入通配的插入方法<<<<<<<<<<<<<<<<<<<<<<<<")
//  val colvalue = line.getAs[String](colconfig.colname)
//  println(colconfig.colname + "<<<<<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>" + colvalue)
//  //      println(line.toSeq.toList)
//
//  val coltype = colconfig.coltype
//  if (colvalue != null) {
//  val value = coltype match {
//  case 12 => PDataType.fromTypeId(colconfig.coltype).toBytes(line.getAs[String](colconfig.colname))
//  case 4 => PDataType.fromTypeId(colconfig.coltype).toBytes(line.getAs[String](colconfig.colname).toInt)
//  case 8 => PDataType.fromTypeId(colconfig.coltype).toBytes(line.getAs[String](colconfig.colname).toDouble)
//  case 93 => PDataType.fromTypeId(colconfig.coltype).toBytes(Timestamp.valueOf(line.getAs[String](colconfig.colname)))
//}
//  put.addColumn(Bytes.toBytes(colfamily), Bytes.toBytes(colconfig.colname), value)
//}
//  else {
//  put
//}
//}
//  utils
//}










//    colconfig.coltype match {
//    case "String" => BytestoString(dataBYTES)
//    case "Timestamp" => BytestoTimestampString(dataBYTES)
//    case "Float" => BytestoFloatString(dataBYTES)
//    case "Double" => BytestoDoubleString(dataBYTES)
//    case "Int" => BytestoIntString(dataBYTES)
//    case "Time" => BytestoTimeString(dataBYTES)
//    case _ => BytestoString(dataBYTES)
//  }






//def BytestoString(hdata: Array[Byte])={
//  if(hdata==null) "-" else Bytes.toString(hdata)
//}
//  def BytestoTimestampString(ptimestamp: Array[Byte])={
//  if(ptimestamp==null) "-" else PTimestamp.INSTANCE.toObject(ptimestamp).toString
//  //  if(ptimestamp==null) "" else PDataType.fromTypeId(93).toObject(ptimestamp).toString
//}
//  def BytestoTimeString(ptime: Array[Byte])={
//  if(ptime==null) "-" else PTime.INSTANCE.toObject(ptime).toString
//}
//
//  def BytestoFloatString(pfloat: Array[Byte])={
//  if(pfloat==null) "-" else PFloat.INSTANCE.toObject(pfloat).toString
//}
//
//  def BytestoDoubleString(pdouble: Array[Byte])={
//  if(pdouble==null) "-" else PFloat.INSTANCE.toObject(pdouble).toString
//}
//
//  def BytestoIntString(pinteger: Array[Byte])={
//  if(pinteger==null) "-" else PInteger.INSTANCE.toObject(pinteger).toString
//}
