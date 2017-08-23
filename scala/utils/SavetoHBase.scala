package utils

/**
  * Created by duan on 2017/4/6.
  */
import java.io.File

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SavetoHBase extends ConfSupport with timeSupport with LogSupport{
  //加载配置文件
  loadconf("/tableSchedule.conf")

  //日志的操作
  val directory = new File("..")
  val filePath = directory.getAbsolutePath //设定为上级文件夹 获取绝对路径
  PropertyConfigurator.configure(filePath + "/conf/log4j.properties")//读取日志配置文件

  //配置spark
  val sparkconf = new SparkConf()
    .setMaster(getconf("SavetoHBase.master"))
    .setAppName(getconf("SavetoHBase.appname"))
    .set("spark.port.maxRetries",getconf("SavetoHBase.portmaxRetries"))
    .set("spark.cores.max",getconf("SavetoHBase.coresmax"))
    .set("spark.executor.memory",getconf("SavetoHBase.executormemory"))
  //启动spark
  val spark = SparkSession.builder().config(sparkconf).getOrCreate()

  def main(args: Array[String]) = {
    log.error(getlabeltime+">>>>>>>>>>>>>>>>>>>>>>>>>>>>进入主程序")
        try{
          savetoHBase.write(args(0))
        }
          catch {
            case e: Exception => {
              e.printStackTrace()
              log.error(getlabeltime+s"!!!!!!!!!!!!!!!!!!!!进程失败")
            }
          }
        finally {
          log.error(getlabeltime+s">>>>>>>>>>>>>>>>>>>>>>>>>进程结束")
          spark.stop()
        }
  }


  object savetoHBase extends HbaseAPI with LogSupport{
    //加载配置文件
    loadconf("/tableSchedule.conf")
    def readCSV(path: String) = {
      val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> path)
      val dataall = spark.read.options(options).format("com.databricks.spark.csv").load()
      log.error(getlabeltime+">>>>>>>>>>>>>>>>>>>>>>>>>读取完毕")
      dataall
    }

    //写入hbase
    def write(dfname: String) = {
      try {
        //获取dataframe的文件名
        log.error(">>>>>>>>文件名："+dfname)
        val dfconfigs = dfname.split("@").map(_.trim)
        val label=dfconfigs(2);log.error(">>>>>>>>标签："+label)
        val tablename = dfconfigs(0);log.error(">>>>>>>>表名："+tablename)
        val dfpath = getconf("SavetoHBase.hdfs_basepath") + dfname;log.error(">>>>>>>>路径："+dfpath)
        //读取hbase配置
        val zokhosts = getconf("SavetoHBase.zokhosts");log.error(">>>>>>>>主机名："+zokhosts)
        val zokmaster= getconf("SavetoHBase.zokmaster");log.error(">>>>>>>>主节点："+zokmaster)
        val zokport = getconf("SavetoHBase.zokport");log.error(">>>>>>>>端口："+zokport)
        //读取postgres配置
        val url=getconf("SavetoHBase.sqlurl");log.error(">>>>>>>>sql链接："+url)
        val user=getconf("SavetoHBase.username");log.error(">>>>>>>>用户名："+user)
        val password=getconf("SavetoHBase.password");log.error(">>>>>>>>密码："+password)
        //对每个dataframe调用流程
        log.error(">>>>>>>>>>>>>>>>>>>>>>>>>进入数据：" + dfname);//保存资产画像的结果
        try {
          if (dfconfigs(2) != "NET.csv" && dfconfigs(2) != "MONI.csv") {
            saveToHbase(readCSV(dfpath),tablename,zokhosts,zokport)
          }
        } catch {
          case e: Exception => log.error(getlabeltime+s"!!!!!!资产画像入库失败!!!!!!!!!!" + e.getMessage)
        }
        try {
          if (dfconfigs(2) == "NET.csv") {
            //读取流量分析的结果
            savenetflowresult(readCSV(dfpath),spark,zokmaster,zokport,url,user,password)
          }
        } catch {
          case e: Exception => log.error(getlabeltime+s"!!!!!!流量分析入库失败!!!!!!!!!!" + e.getMessage)
        }
        try {
          if (dfconfigs(2) == "MONI.csv") {
            //读取网络审计分析的结果
            savemoniflowresult(readCSV(dfpath),spark,url,user,password)
          }
        } catch {
          case e: Exception => log.error(getlabeltime+s"!!!!!!网络审计分析入库失败!!!!!!!!!!" + e.getMessage)
        }
      }catch {
        case e:Exception=>log.error(s"!!!!!!入库失败!!!!!!!!!!" +dfname+ e.getMessage)
      }
    }

  }

}



















//val ctx = new SCPClient()
//val  thread = new Thread(new loader)
//
//class loader extends Runnable{
//  override def run(): Unit = {
//    while(true) {
//
//
//
//
//      Thread.sleep(6000)
//    }
//  }
//}
