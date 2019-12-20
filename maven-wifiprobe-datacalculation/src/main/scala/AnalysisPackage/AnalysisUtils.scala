package AnalysisPackage

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

object AnalysisUtils {
  /*计算用的一些逻辑方法实现*/


  /**
   * 得到mac地址的前缀（6位）
   */
   def getprefix(mac:String):String={
     if (mac.contains(":")){
     val t1=mac.split(":")(0)
     val t2=mac.split(":")(1)
     val t3=mac.split(":")(2)
     t1.toUpperCase+t2.toUpperCase+t3.toUpperCase}
     else {
       "empty"
     }

   }
  /**
   * 根据时间字符串获取时间秒数,单位(秒) 时间戳是指格林威治时间1970年01月01日00时00分00秒(北京时间1970年01月01日08时00分00秒)起至现在的总毫秒数
   * 所以返回时间戳/1000
   **/
  def getTimestampByString(timeString: String): Long = {
    val sf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sf.parse(timeString).getTime / 1000
  }

  /**
   * 获得当前日期的后一天
   */
  def getAfterDay(day:String):String={
    val sf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val startDayDate=sf.parse(day)
    val c=Calendar.getInstance()
    c.setTime(startDayDate)
    val day1=c.get(Calendar.DATE)
    c.set(Calendar.DATE,day1+1)
    val dayAfter= new SimpleDateFormat("yyyy-MM-dd").format(c.getTime)
    dayAfter
  }

  /**
   * 计算时间差
   */
  def getTimeLag(timeStart:String,timeEnd:String):String={
    val sf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time=sf.parse(timeEnd).getTime-sf.parse(timeStart).getTime
    val d=time/86400000
    val h=time%86400000/3600000
    val m=time%86400000%3600000/60000
    val s=time%86400000%3600000%60000/1000
    if(time<0){
      "0"
    }
    else {
      d.toString+"天"+h.toString+"小时"+m.toString+"分钟"+s.toString+"秒"

    }
  }

  /**
   * 根据日期获得当天是这个月的第几周
   */
  def getWeekBydata(day:String):(String,Int)={
    val sf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val DayDate=sf.parse(day)
    val c=Calendar.getInstance()
    c.setTime(DayDate)
    val year=day.split("-")(0)
    (year,c.get(Calendar.WEEK_OF_YEAR))
  }

  /**
   * 计算指定某天的各时段人流量
   */
  def OneDayCount(day:String,date:RDD[(String,String)],sc:SparkContext):(RDD[(String,Long)],RDD[((String,String),Int)])={
    val eachDayRDD=date.filter(_._2.contains(day.toString))
    var (everydaycountRDD,eachdaymacRDD)=EveryDayCountBytime.usercountbytime(eachDayRDD,day.toString,sc)
    //对mac地址做统计，算出一天内每个mac地址出现的次数
    eachdaymacRDD=eachdaymacRDD.reduceByKey(_+_)
    /*everydaycountRDD.foreach(println)*/
    (everydaycountRDD,eachdaymacRDD)

  }

  /**
   * 计算某年某月第几周内常客非常客数量
   */
  def EachWeekMacCount(eachdaymacRDD: RDD[((String,String),Int)],sc:SparkContext):
  (RDD[((String,Int),Int,Long)],RDD[((String,(String,Int)),Int)])= {
    //eachdaybyweekmacRDD将每一天日期映射的某一月的第几周
    val eachdaybyweekmacRDD = eachdaymacRDD.map(x =>
      ((x._1._2, AnalysisUtils.getWeekBydata(x._1._1)), x._2))
    //eachweekmacRDD存储某年某月第几周某个mac地址出现的次数
    val eachweekmacRDD = eachdaybyweekmacRDD.reduceByKey(_ + _)
    //eachweekmacandcharkedRDD用于存储某年某月第几周的每位mac地址的常客标识
    val eachweekmacandcharkedRDD = eachweekmacRDD.map(x =>
      (x._1._2, {
        if (x._2 >= 7) 1
        else 0
      })
    )
    val groupRDD = eachweekmacandcharkedRDD.countByValue()
    var arr=new ArrayBuffer[(((String,Int),Int),Long)]()

    for((key,value)<-groupRDD){
      val arr1=Array((key,value))
      arr=arr.++=(arr1)
    }
    val regularuserCountRDD=sc.parallelize(arr).map(x=>(x._1._1,x._1._2,x._2))

   (regularuserCountRDD,eachweekmacRDD)
  }


}
