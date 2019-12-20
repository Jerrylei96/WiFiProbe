
import AnalysisPackage.{AnalysisUtils, MacToBrandMap, getSparkContext}
import DataReadAndWrite.{DataReadToRDD, DataWriteToHive}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer


object ScalaAnalysisMain {

  def main(args: Array[String]): Unit = {
    //创建sparkContext,HiveContext
    val sc=getSparkContext.getLocalSC("wifiprobe")

    /**
     * 1.测试用数据
     */
    val testRDD=DataReadToRDD.gettestRDD(sc)
    testRDD.cache()
    /**
     * 1.计算一段时间内的每天按时间段客流量统计
     * 2.计算一段时间内的每天每个mac地址出现的次数
     */
    val startday=testRDD.first()._2.split(" ")(0)
    val endDay="2019-02-26"
  //eachdaycountbytimeRDD存储每天内按时间段统计的结果,
  //eachdaymacRDD存储每天内每个mac地址出现的次数
  // 在这里赋初值
    var (eachdaycountbytimeRDD,eachdaymacRDD)=AnalysisUtils.OneDayCount(startday,testRDD,sc)
    val theRDD1=eachdaycountbytimeRDD.map(x=>x._2)
    var sum1:Long= theRDD1.reduce((x,y)=>x+y)
    val arr1=Array((startday,sum1))
  //eachdaysumcountRDD表示每一天人流量总和的统计结果,在这里赋处值
    var eachdaysumcountRDD:RDD[(String,Long)]=sc.parallelize(arr1)
  //日期向后移动一天
    var day=AnalysisUtils.getAfterDay(startday)

    while (AnalysisUtils.getTimestampByString(day+" 00:00:00")<AnalysisUtils.getTimestampByString(endDay+" 00:00:00")){
      val (onedaycountbytimeRDD,onedaymacRDD)=AnalysisUtils.OneDayCount(day,testRDD,sc)
    //计算这一天的总客流量
      var theRDD=onedaycountbytimeRDD.map(x=>x._2)
      var sum:Long= theRDD.reduce((x,y)=>x+y)
      eachdaycountbytimeRDD.map(x=> (sum=sum+ x._2))
      var arr=Array((day,sum))
      var onedaysumcountRDD=sc.parallelize(arr)

    //将这一天的每个时间段的数据加入到整个的结果中
      eachdaycountbytimeRDD=eachdaycountbytimeRDD.union(onedaycountbytimeRDD)
    //将这一天的mac地址统计加入到整个的结果eachdaymacRDD中
      eachdaymacRDD=eachdaymacRDD.union(onedaymacRDD)
    //将这一天的总客流量结果加入到eachdaysumcountRDD中
      eachdaysumcountRDD=eachdaysumcountRDD.union(onedaysumcountRDD)
    //日期向后移动一天
      day=AnalysisUtils.getAfterDay(day)
  }
    eachdaymacRDD.cache()
    eachdaycountbytimeRDD.repartition(1).saveAsTextFile("./maven-wifiprobe-datacalculation/src/main/result/resulttext/")
    eachdaysumcountRDD.repartition(1).saveAsTextFile("./maven-wifiprobe-datacalculation/src/main/result/resulttext1/")
    eachdaymacRDD.repartition(1).saveAsTextFile("./maven-wifiprobe-datacalculation/src/main/result/resulttext2/")
    DataWriteToHive.everydaycountdatasavetoHive(sc,eachdaycountbytimeRDD,"db_hivetest","each_day_count_by_time_result")
    DataWriteToHive.everydaycountdatasavetoHive(sc,eachdaysumcountRDD,"db_hivetest","each_day_sum_count_result")

  /*
  3.计算用户活跃度
   */
  //eachweekmacRDD存储某年某月第几周某个mac地址出现的次数
  //regularuserCountRDD存储某年某月第几周内常客数量和非常客数量
    val (regularuserCountRDD,eachweekeachmaccountRDD)=AnalysisUtils.EachWeekMacCount(eachdaymacRDD,sc)
    val eachweekeachmaccountRDD1=eachweekeachmaccountRDD.map(x=>(x._1._1,x._1._2._1+"-"+x._1._2._2.toString,x._2))
    val regularuserCountRDD1=regularuserCountRDD.map(x=>(x._1._1+"-"+x._1._2.toString,x._2,x._3))
    eachweekeachmaccountRDD1.repartition(1).saveAsTextFile("./maven-wifiprobe-datacalculation/src/main/result/resulttext5/")
    regularuserCountRDD1.repartition(1).saveAsTextFile("./maven-wifiprobe-datacalculation/src/main/result/resulttext4/")
    DataWriteToHive.eachweekeachmaccountdatasavetoHive(sc,eachweekeachmaccountRDD1,"db_hivetest","each_week_each_mac_count_result")
    DataWriteToHive.regularuserCountdatasavetoHive(sc,regularuserCountRDD1,"db_hivetest","regular_user_count_result")

    /*
    4.来访周期
   */
    val timetoeachmacRDD=testRDD.groupBy(_._1)
    val timetoeachmacRDD1=timetoeachmacRDD.map(x=>{
        (x._1,x._2.takeRight(2).map(y=>(y._2)))
        }).map(z=>(z._1,z._2.head,z._2.last))
    val timelagtoeachmacRDD=timetoeachmacRDD1.map(
      x=>{
        (x._1,AnalysisUtils.getTimeLag(x._2,x._3))
      })
    timelagtoeachmacRDD.repartition(1).saveAsTextFile("./maven-wifiprobe-datacalculation/src/main/result/resulttext7/")
    DataWriteToHive.timelagtoeachmacdatasavetoHive(sc,timelagtoeachmacRDD,"db_hivetest","time_lag_to_each_mac_result")

     /*
      5.品牌统计
     */
  //macmapfile读取mac对应表存储为RDD：macmaptableRDD
    val macmapfile=sc.textFile("./maven-wifiprobe-datacalculation/src/main/scala/resources/mac.txt")
    val macmaptableRDD=macmapfile.map(row=>{
    val mac=row.split(" ")(0)
    val brand=row.split(" ")(1).toLowerCase
    (mac,brand)
        })
    macmaptableRDD.repartition(1).saveAsTextFile("./maven-wifiprobe-datacalculation/src/main/result/resulttext9/")
  //macmaptable将RDD本地化为Array
    val macmaptable=macmaptableRDD.collect()
  //macRDD存储出现过的所有用户的mac地址
    val macRDD=testRDD.map(x=>x._1).distinct()
  //brandtomacRDD存储将mac对应的厂商品牌
    val brandtomacRDD=macRDD.map(x=>{
      (x,MacToBrandMap.getBrandFromMac(x,macmaptable))
    })
  //brandcount将品牌提取出来并且计数
    val brandcount=brandtomacRDD.map(x=>x._2).countByValue()
    var arr=new ArrayBuffer[(String,Long)]()
    for((key,value)<-brandcount){
      val arr1=Array((key,value))
      arr=arr.++=(arr1)
     }
    val brandcountRDD=sc.parallelize(arr)
    brandcountRDD.repartition(1).saveAsTextFile("./maven-wifiprobe-datacalculation/src/main/result/resulttext8/")
    DataWriteToHive.brandcountdatasavetoHive(sc,brandcountRDD,"db_hivetest","brand_count_result")

    sc.stop()
  }
}
