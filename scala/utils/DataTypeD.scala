package utils

import java.sql.Timestamp

/**
  * Created by duan on 2017/4/28.
  */

  case class sqlConfig(url:String,username:String,password:String)extends Serializable//关系型sql的配置
  case class ColConfig(colname:String,coltype:Int)extends Serializable//第二个参数是phoenix表里面类型的标号 12代表string 93代表timestamp 4代表int
  case class timeConfig(day:String,startTime:Long,endTime:Long) extends Serializable
  case class TrainNET(ATTACKTYPE:String,SPANDTIME:String,TCP:String,UDP:String,PACKERNUM:String,BYTESIZE:String,FLOWDIRECTION:String,SAME:String,RECORDID:String,SRCIP:String,DSTIP:String) extends Serializable
  case class TrainMONI(label:String,urltext:String) extends Serializable
  case class RAWNET(SPANDTIME:Double, TCP:Double, UDP:Double,PACKERNUM:Double,BYTESIZE:Double,PROTO:String,SRCIP:String,DSTIP:String,RECORDTIME:String,ROW:String,SRCPORT:String,DSTPORT:String,target:Int)extends Serializable{}
  case class RESNET(SRCIP:String,DSTIP:String,ATTACKTYPE:String,RECORDTIME:String,PROTO:String,SRCPORT:String,DSTPORT:String,FLOWNUMS:Int)extends Serializable{}
//  case class RESNET(ROW:String,SRCIP:String,DSTIP:String,ATTACKTYPE:String,RECORDTIME:String,COSTTIME:String,PROTO:String,PACKERNUM:String,BYTESIZE:String,FLOWDIRECTION:String,SRCPORT:String,DSTPORT:String,FLOWNUMA:String)extends Serializable{}
  case class ENDNET(SRCIP:String,DSTIP:String,ATTACKTYPE:String,RECORDTIME:Timestamp,PROTO:String,SRCPORT:Int,DSTPORT:Int,FLOWNUMS:Int,FLOWNUMA:Int)extends Serializable{}
//  case class ENDNET(ROW:String,SRCIP:String,DSTIP:String,ATTACKTYPE:String,RECORDTIME:Timestamp,COSTTIME:Float,PROTO:String,PACKERNUM:Int,BYTESIZE:Int,FLOWDIRECTION:String,SRCPORT:Int,DSTPORT:Int,FLOWNUMA:Int)extends Serializable{}
  case class RAWMONI(id:String, date:String, srcip:String, dstip:String, uri:String, urltext:String,url:String)extends Serializable{}
  case class RESMONI(id:String,date:String,srcip:String,dstip:String,uri:String,url:String,predictionnb:String,probabilitynb:String,predictionlr:String,probabilitylr:String)extends Serializable
  case class ENDMONI(id:String,date:Timestamp,srcip:String,dstip:String,uri:String,url:String,predictionnb:Double,probabilitynb:Double,predictionlr:Double,probabilitylr:Double)extends Serializable{}
//hbase获取的数据类型
  case class NETFLOWHData(STARTTIME:String,ENDTIME:String,PROTO:String,PACKERNUM:String,BYTESIZE:String,SRCIP:String,DSTIP:String,RECORDTIME:String,ROW:String,SRCPORT:String,DSTPORT:String,FLAG:String="")extends Serializable{
    def getflag={
      if(STARTTIME=="-"||ENDTIME=="-"||PROTO=="-"||PACKERNUM=="-"||BYTESIZE=="-"||SRCIP=="-"||DSTIP=="-"||RECORDTIME=="-"||ROW=="-"||SRCPORT=="-"||DSTPORT=="-") "0" else "1"
    }
  }
  case class MONIFLOWHData(ROW:String,DATE:String,SRCIP:String,DSTIP:String,APPPROTO:String,METHOD:String,SRCPORT:String,DSTPORT:String,URI:String,HOST:String,FLAG:String="")extends Serializable{
    def getflag={
      if(ROW=="-"||DATE=="-"||SRCIP=="-"||DSTIP=="-"||APPPROTO=="-"||METHOD=="-"||SRCPORT=="-"||DSTPORT=="-"||URI=="-"||HOST=="-") "0" else "1"
    }
  }
  case class GLOGHData(RECORDID:String,EVENTDETAIL:String,SOURCEIP:String,DESTIP:String,FIRSTRECVTIME:String,FLAG:String="")extends Serializable{
    def getflag={
      if(RECORDID=="-"||EVENTDETAIL=="-"||SOURCEIP=="-"||DESTIP=="-"||FIRSTRECVTIME=="-") "0" else "1"
    }
  }
  case class EVENTRESHData(ROW:String,LOGID:String,EVENT_RULE_ID:String,EVENT_RULE_NAME:String,SOURCEIP:String,DESTIP:String,OPENTIME:String,FLAG:String="")extends Serializable{
    def getflag={
      if(ROW=="-"||LOGID=="-"||EVENT_RULE_ID=="-"||EVENT_RULE_NAME=="-"||SOURCEIP=="-"||DESTIP=="-"||OPENTIME=="-") "0" else "1"
    }
  }
  case class EVENTRULEHData(EVENT_RULE_ID:String,EVENT_SUB_TYPE:String,FLAG:String="")extends Serializable{
    def getflag={
      if(EVENT_RULE_ID=="-"||EVENT_SUB_TYPE=="-") "0" else "1"
    }
  }
  case class EVENTTYPEHData(EVENT_TYPE_ID:String,EVENT_TYPE_NAME:String,FLAG:String="")extends Serializable{
    def getflag={
      if(EVENT_TYPE_ID=="-"||EVENT_TYPE_NAME=="-") "0" else "1"
    }
  }
  case class TESTROW(COSTTIME:String,TCP:String,UDP:String,ICMP:String,GRE:String,PROTO:String,PACKERNUM:String,BYTESIZE:String,SRCIP:String,DSTIP:String,RECORDTIME:String,ROW:String,SRCPORT:String,DSTPORT:String,FLAG:String="")extends Serializable{
    def getflag={
      if(COSTTIME=="-"||TCP=="-"||UDP=="-"||ICMP=="-"||GRE=="-"||PROTO=="-"||PACKERNUM=="-"||BYTESIZE=="-"||SRCIP=="-"||DSTIP=="-"||RECORDTIME=="-"||ROW=="-"||SRCPORT=="-"||DSTPORT=="-") "0" else "1"
    }
  }
  case class EStest(label:String,urltext:String)extends Serializable