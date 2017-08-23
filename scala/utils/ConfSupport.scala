package utils

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Try

/**
  * Created by duan on 2017/5/28.
  */
trait ConfSupport {
    var config: Config = _
    var configInner:Config = _
    var configSystem: Config = _
    // 从本地的路径 和 服务器路径分别加载conf文件， 取outer join， 冲突的key以服务器上的为准
    def loadconf(confPath:String) = {
      println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>加载配置")
      val systemFile = new File(".."+ File.separator +"conf")
      val system: Array[String] = Try{systemFile.listFiles().map(_.getAbsolutePath).filter(_.endsWith(s".conf"))}.getOrElse(Array())
      if (system.length == 0) {
        config = ConfigFactory.parseURL(this.getClass().getResource(confPath))
      } else {
        configInner = ConfigFactory.parseURL(this.getClass().getResource(confPath))
        val conf2 = system.map(x => ConfigFactory.parseFile(new File(x)))
        if (conf2.length > 1) {
          configSystem = conf2.foldRight(conf2.head)((x,y) => x.withFallback(y))
        } else {
          configSystem = Try(conf2(0)).getOrElse(null)
        }
        config = configSystem.withFallback(configInner)
      }
    }

  def getconf(key:String)={
    this.config.getString(key).trim
  }
}

