package AnalysisPackage

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MacToBrandMap {
  def getBrandFromMac(mac:String,macmaptable:Array[(String,String)]):String={
    val prefix=AnalysisUtils.getprefix(mac)
    if(prefix!="empty"){
    val brand=macmaptable.filter(x=>{x._1==(prefix)})
    val result=brand.map(_._2)
    if(result.isEmpty){
      "empty"
    }else{
      result(0).toString
    }}
    else {
      "empty"
    }
  }

}
