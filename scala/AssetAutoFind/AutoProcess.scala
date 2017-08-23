package AssetAutoFind

import java.io.{File, Serializable}

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import utils.{ConfSupport, LogSupport, sqlConfig, timeSupport}

/**
  * Created by duan on 2017/7/28.
  */

object AutoProcess extends ConfSupport with AssetINFOUtils with Serializable with LogSupport with timeSupport {
  //  加载配置
  loadconf("/models.conf")
  //  日志
  val directory = new File("..")
  val filePath = directory.getAbsolutePath //设定为上级文件夹 获取绝对路径
  PropertyConfigurator.configure(filePath + "/conf/log4j.properties")
  def  main(args: Array[String]) = {

    log.error("######### 资产自动发现开始运行于" + getlabeltime)

    //配置spark
    val sparkconf = new SparkConf()
      .setMaster(getconf("AssetAutoFind.master"))
      .setAppName(getconf("AssetAutoFind.appname"))
      .set("spark.port.maxRetries", getconf("AssetAutoFind.portmaxRetries"))
      .set("spark.cores.max", getconf("AssetAutoFind.coresmax"))
      .set("spark.executor.memory", getconf("AssetAutoFind.executormemory"))
      .set("es.nodes", getconf("AssetAutoFind.esnodes"))
      .set("es.port", getconf("AssetAutoFind.esport"))
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()


    //程序主体
    try {
      //获取访问数据
      val netflowDF = getDateByScan(spark,args)
      log.error(s"######### 这段时间内的所有访问数据总数为==>{" + netflowDF.count() + "}")
      //  postgres的配置
      val sqlConfigs = sqlConfig(getconf("AssetAutoFind.sql_url"), getconf("AssetAutoFind.sql_username"), getconf("AssetAutoFind.sql_password"))
      //获得资产库以及已经发现资产的IP
      val neips = getNeIp(spark, sqlConfigs)
      log.error(s"######### 资产库以及已经被发现的资产总数为==>{" + neips.length + "}")

      netflowDF.persist(StorageLevel.MEMORY_AND_DISK_SER)

      //筛选出新发现资产
      val newDFs = getNewAsset(spark, netflowDF, neips)
      log.error("根据源IP发现的资产")
      newDFs._1.show(10, false)
      log.error("根据目的IP发现的资产")
      newDFs._2.show(10, false)

      //新资产入库
      saveNewAsset(spark, newDFs, sqlConfigs)

    }
    catch {
      case e: Exception =>
        log.error("!!!!!!!资产自动发现程序出错" + e.getMessage)
    }
    finally {
      //停止spark
      spark.stop()
      log.error(s"######### 进程结束")
    }
    log.error("######### 资产自动发现运行结束于" + getlabeltime)
  }
}

