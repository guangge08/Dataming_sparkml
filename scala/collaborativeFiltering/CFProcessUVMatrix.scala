package collaborativeFiltering

import org.apache.spark.sql.{SQLContext, SaveMode}
import CFEnvironmentSetup._

/**
  * Created by Administrator on 2017/4/28 0028.
  */

object CFProcessUVMatrix {
  def readUV(dataPath: String, sqlC: SQLContext): Unit ={
    val readOption = Map("header" -> "true", "delimiter" -> ",", "path" -> dataPath)
    val uvMatrix = sqlC.read.format("com.databricks.spark.csv")
    .options(readOption).load
    uvMatrix

  }

  def main(args: Array[String]): Unit ={

    //设置spark
    val spark = getLocalSparkSession()
    val sqlC = spark.sqlContext

    /*
    关联相似度表格
     */
    val simPath = "G:\\landun\\蓝基\\推荐算法_资产画像\\0413allTable\\rowsim.csv"
    val ipListPath = "G:\\landun\\蓝基\\推荐算法_资产画像\\0413allTable\\ipList.csv"
    val storePath = "G:\\landun\\蓝基\\推荐算法_资产画像\\0413allTable\\connedSim.csv"
    connSim(simPath, ipListPath, storePath, sqlC)

    /*
    计算UV矩阵
     */


  }



  def connSim(simPath:String, ipListPath: String, storePath:String, sqlC: SQLContext): Unit ={
    //读取文件的设置
    val simReadOption = Map("header" -> "true", "delimiter" -> ",", "path" -> simPath)
    val csvFormat = "com.databricks.spark.csv"
    val ipListReadOption = Map("header" -> "true", "delimiter" -> ",", "path"-> ipListPath)
    //读取文件
    val simTable = sqlC.read.format(csvFormat).options(simReadOption).load
    val ipListTable = sqlC.read.format(csvFormat).options(ipListReadOption).load
    //sim: IP1, IP2, SIM;   iplist: IP, INDEX
    simTable.createOrReplaceTempView("simTable")
    ipListTable.createOrReplaceTempView("ipListTable")

    //关联IP1
    val connedTable1 = sqlC.sql(s"SELECT lt.IP AS IPADDRESS1, IP1, IP2, SIM FROM" +
      s" simTable sim LEFT JOIN ipListTable lt ON lt.INDEX = sim.IP1 ")
    connedTable1.createOrReplaceTempView("connedTable1")

    //关联IP2
    val connedTable2 = sqlC.sql(s"SELECT IPaddress1,lt.IP IPaddress2, SIM, IP1, IP2 FROM" +
      s" connedTable1 c1 LEFT JOIN ipListTable lt ON lt.INDEX = c1.IP2 ")

    connedTable2.show(50)
    //保存文件
    val saveOption = Map("header" ->"true", "delimiter" -> ",", "path" -> storePath)
    connedTable2.write.options(saveOption).mode(SaveMode.Overwrite).format(csvFormat).save


  }

}
