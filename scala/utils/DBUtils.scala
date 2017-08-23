package utils

import java.sql.{Connection, DriverManager}

/**
  * Created by duan on 2017/2/13.
  */
class DBUtils extends Serializable{
  def getPhoniexConnect(connUrl:String):Connection={
    val url:String = "jdbc:phoenix:" + connUrl
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    val conn:Connection = DriverManager.getConnection(url)
    conn
  }
  def getSQLConnect(sqlurl:String,username:String,password:String)={
    val driver = "org.postgresql.Driver"
    Class.forName(driver)
    var conn:Connection = DriverManager.getConnection(sqlurl, username, password)
    conn
  }
}
