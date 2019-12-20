package AnalysisPackage

import AnalysisPackage.AnalysisUtils.getTimestampByString
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object EveryDayCountBytime {


  //1.客流量统计,maccountRDD统计每个mac地址出现（没有汇总，用于用户活跃度）
  def usercountbytime(rdd:RDD[(String,String)],day1:String,sc:SparkContext):(RDD[(String,Long)],RDD[((String,String),Int)])={
    val day=day1
    val rddfiltered1=rdd.map(x=>(x._1,getTimestampByString(x._2)))
      .filter(_._2>=getTimestampByString(day+" 07:00:00"))
      .filter(_._2<getTimestampByString(day+" 07:30:00"))
      .map(y=>((day,y._1),1)).distinct
    val rddfiltered2=rdd.map(x=>(x._1,getTimestampByString(x._2)))
      .filter(_._2>=getTimestampByString(day+" 07:30:00"))
      .filter(_._2<getTimestampByString(day+" 08:00:00"))
      .map(y=>((day,y._1),1)).distinct
    val rddfiltered3=rdd.map(x=>(x._1,getTimestampByString(x._2)))
      .filter(_._2>=getTimestampByString(day+" 08:00:00"))
      .filter(_._2<getTimestampByString(day+" 08:30:00"))
      .map(y=>((day,y._1),1)).distinct
    val rddfiltered4=rdd.map(x=>(x._1,getTimestampByString(x._2)))
      .filter(_._2>=getTimestampByString(day+" 08:30:00"))
      .filter(_._2<getTimestampByString(day+" 09:00:00"))
      .map(y=>((day,y._1),1)).distinct
    val rddfiltered5=rdd.map(x=>(x._1,getTimestampByString(x._2)))
      .filter(_._2>=getTimestampByString(day+" 09:00:00"))
      .filter(_._2<getTimestampByString(day+" 09:30:00"))
      .map(y=>((day,y._1),1)).distinct
    val rddfiltered6=rdd.map(x=>(x._1,getTimestampByString(x._2)))
      .filter(_._2>=getTimestampByString(day+" 09:30:00"))
      .filter(_._2<getTimestampByString(day+" 10:00:00"))
      .map(y=>((day,y._1),1)).distinct
    val rddfiltered7=rdd.map(x=>(x._1,getTimestampByString(x._2)))
      .filter(_._2>=getTimestampByString(day+" 10:00:00"))
      .filter(_._2<getTimestampByString(day+" 10:30:00"))
      .map(y=>((day,y._1),1)).distinct
    val rddfiltered8=rdd.map(x=>(x._1,getTimestampByString(x._2)))
      .filter(_._2>=getTimestampByString(day+" 10:30:00"))
      .filter(_._2<getTimestampByString(day+" 11:00:00"))
      .map(y=>((day,y._1),1)).distinct
    val rddfiltered9=rdd.map(x=>(x._1,getTimestampByString(x._2)))
      .filter(_._2>=getTimestampByString(day+" 11:00:00"))
      .filter(_._2<getTimestampByString(day+" 11:30:00"))
      .map(y=>((day,y._1),1)).distinct
    val rddfiltered10=rdd.map(x=>(x._1,getTimestampByString(x._2)))
      .filter(_._2>=getTimestampByString(day+" 11:30:00"))
      .filter(_._2<getTimestampByString(day+" 12:00:00"))
      .map(y=>((day,y._1),1)).distinct
    val rddfiltered11=rdd.map(x=>(x._1,getTimestampByString(x._2)))
      .filter(_._2>=getTimestampByString(day+" 12:00:00"))
      .filter(_._2<getTimestampByString(day+" 12:30:00"))
      .map(y=>((day,y._1),1)).distinct

    val rddfiltered12=rdd.map(x=>(x._1,getTimestampByString(x._2)))
      .filter(_._2>=getTimestampByString(day+" 12:30:00"))
      .filter(_._2<getTimestampByString(day+" 13:00:00"))
      .map(y=>((day,y._1),1)).distinct

    val rddfiltered13=rdd.map(x=>(x._1,getTimestampByString(x._2)))
      .filter(_._2>=getTimestampByString(day+" 13:00:00"))
      .filter(_._2<getTimestampByString(day+" 13:30:00"))
      .map(y=>((day,y._1),1)).distinct

    val rddfiltered14=rdd.map(x=>(x._1,getTimestampByString(x._2)))
      .filter(_._2>=getTimestampByString(day+" 13:30:00"))
      .filter(_._2<getTimestampByString(day+" 14:00:00"))
      .map(y=>((day,y._1),1)).distinct
    val rddfiltered15=rdd.map(x=>(x._1,getTimestampByString(x._2)))
      .filter(_._2>=getTimestampByString(day+" 14:00:00"))
      .filter(_._2<getTimestampByString(day+" 14:30:00"))
      .map(y=>((day,y._1),1)).distinct
    val rddfiltered16=rdd.map(x=>(x._1,getTimestampByString(x._2)))
      .filter(_._2>=getTimestampByString(day+" 14:30:00"))
      .filter(_._2<getTimestampByString(day+" 15:00:00"))
      .map(y=>((day,y._1),1)).distinct
    val rddfiltered17=rdd.map(x=>(x._1,getTimestampByString(x._2)))
      .filter(_._2>=getTimestampByString(day+" 15:00:00"))
      .filter(_._2<getTimestampByString(day+" 15:30:00"))
      .map(y=>((day,y._1),1)).distinct
    val rddfiltered18=rdd.map(x=>(x._1,getTimestampByString(x._2)))
      .filter(_._2>=getTimestampByString(day+" 15:30:00"))
      .filter(_._2<getTimestampByString(day+" 16:00:00"))
      .map(y=>((day,y._1),1)).distinct
    val rddfiltered19=rdd.map(x=>(x._1,getTimestampByString(x._2)))
      .filter(_._2>=getTimestampByString(day+" 16:00:00"))
      .filter(_._2<getTimestampByString(day+" 16:30:00"))
      .map(y=>((day,y._1),1)).distinct
    val rddfiltered20=rdd.map(x=>(x._1,getTimestampByString(x._2)))
      .filter(_._2>=getTimestampByString(day+" 16:30:00"))
      .filter(_._2<getTimestampByString(day+" 17:00:00"))
      .map(y=>((day,y._1),1)).distinct
    val rddfiltered21=rdd.map(x=>(x._1,getTimestampByString(x._2)))
      .filter(_._2>=getTimestampByString(day+" 17:00:00"))
      .filter(_._2<getTimestampByString(day+" 17:30:00"))
      .map(y=>((day,y._1),1)).distinct
    val rddfiltered22=rdd.map(x=>(x._1,getTimestampByString(x._2)))
      .filter(_._2>=getTimestampByString(day+" 17:30:00"))
      .filter(_._2<getTimestampByString(day+" 18:00:00"))
      .map(y=>((day,y._1),1)).distinct
    val rddfiltered23=rdd.map(x=>(x._1,getTimestampByString(x._2)))
      .filter(_._2>=getTimestampByString(day+" 18:00:00"))
      .filter(_._2<getTimestampByString(day+" 18:30:00"))
      .map(y=>((day,y._1),1)).distinct
    val rddfiltered24=rdd.map(x=>(x._1,getTimestampByString(x._2)))
      .filter(_._2>=getTimestampByString(day+" 18:30:00"))
      .filter(_._2<getTimestampByString(day+" 19:00:00"))
      .map(y=>((day,y._1),1)).distinct

    val array=Array((day+" 07:00:00",rddfiltered1.count()),(day+" 07:30:00",rddfiltered2.count()),(day+" 08:00:00",rddfiltered3.count()),
      (day+" 08:30:00",rddfiltered4.count()),(day+" 09:00:00",rddfiltered5.count()),(day+" 09:30:00",rddfiltered6.count()),
      (day+" 10:00:00",rddfiltered7.count()),(day+" 10:30:00",rddfiltered8.count()),(day+" 11:00:00",rddfiltered9.count()),
      (day+" 11:30:00",rddfiltered10.count()),(day+" 12:00:00",rddfiltered11.count()),(day+" 12:30:00",rddfiltered12.count()),
      (day+" 13:00:00",rddfiltered13.count()),(day+" 13:30:00",rddfiltered14.count()),(day+" 14:00:00",rddfiltered15.count()),
      (day+" 14:30:00",rddfiltered16.count()),(day+" 15:00:00",rddfiltered17.count()),(day+" 15:30:00",rddfiltered18.count()),
      (day+" 16:00:00",rddfiltered19.count()),(day+" 16:30:00",rddfiltered20.count()),(day+" 17:00:00",rddfiltered21.count()),
      (day+" 17:30:00",rddfiltered22.count()),(day+" 18:00:00",rddfiltered23.count()),(day+" 18:30:00",rddfiltered24.count())
    )
    val maccountRDD=rddfiltered1.union(rddfiltered2).union(rddfiltered3).union(rddfiltered4).union(rddfiltered5)
      .union(rddfiltered6).union(rddfiltered7).union(rddfiltered8).union(rddfiltered9).union(rddfiltered10)
      .union(rddfiltered11).union(rddfiltered12).union(rddfiltered13).union(rddfiltered14).union(rddfiltered15)
      .union(rddfiltered16).union(rddfiltered17).union(rddfiltered18).union(rddfiltered19).union(rddfiltered20)
      .union(rddfiltered21).union(rddfiltered22).union(rddfiltered23).union(rddfiltered24)
    (sc.parallelize(array),maccountRDD)
  }
}
