package utils

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}


import scala.util.Try

/**
  * Created by duan on 2017/6/2.
  */
trait timeSupport {
  def getlabeltime={
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
  }
  def getStartEndTime(args: Array[String])={
    //获得取得数据的起止时间
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, -1)
    val time = cal.getTime
    val yestoday = Try(args(0).trim).getOrElse(new SimpleDateFormat("yyyy-MM-dd").format(time))
    val today = Try(args(1).trim).getOrElse(new SimpleDateFormat("yyyy-MM-dd").format(new Date))
    val startTime = Timestamp.valueOf(yestoday + " 00:00:00").getTime
    val endTime = Timestamp.valueOf(today + " 00:00:00").getTime
    timeConfig(yestoday+"--"+today,startTime,endTime)
  }

  def getTimeRangeOneHour(args: Array[String]) = {
    //获得取得数据的起止时间
    val cal = Calendar.getInstance
    cal.add(Calendar.HOUR, -1)
    val time = cal.getTime
    val yestoday = Try(args(0).trim).getOrElse(new SimpleDateFormat("yyyy-MM-dd HH").format(time))
    val today = Try(args(1).trim).getOrElse(new SimpleDateFormat("yyyy-MM-dd HH").format(new Date))
    val startTime = Timestamp.valueOf(yestoday + ":00:00").getTime
    val endTime = Timestamp.valueOf(today + ":00:00").getTime
    (startTime,endTime)
  }

}
