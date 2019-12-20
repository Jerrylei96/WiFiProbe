package AnalysisPackage

import org.apache.spark.{SparkConf, SparkContext}

object getSparkContext {
  /**
   * 本地开发模式，不需要连接spark集群
   * local[2] 表示在本机以两线程运行
   * @param appName 自定义应用名字
   * @return
   */
  def getLocalSC(appName: String): SparkContext = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName(appName)
    new SparkContext(conf)
  }

  /**
   * 集群开发模式，程序提交到spark集群进行运行，setJars设定此项目打包后的位置，
   * 打包中依赖不需要spark-assembly-1.4.0-hadoop2.6.0.jar
   * @param appName 自定义应用名字
   * @return
   */
  def getCloudSC(appName: String): SparkContext = {
    val conf: SparkConf = new SparkConf()
      .setMaster("spark://slave1:7077")
      .setAppName(appName)
      .setJars(Array("/home/slave1/IdeaProjects/WifiProbe_scalaAnalysis.jar"))
    new SparkContext(conf)
  }


}
