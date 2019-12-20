package DataReadAndWrite

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext


object DataWriteToHive {
  //写入人流量结果表，2个
  def everydaycountdatasavetoHive(sc:SparkContext, data:RDD[(String,Long)], databasename:String, tablename:String): Unit ={
    val hc = new HiveContext(sc)
    hc.sql("use "+databasename)
    hc.sql("create table "+tablename+"(time string,countnum double) row format delimited fields terminated by '/t'")
    import hc.implicits._
    val dataDF=data.toDF()
    dataDF.registerTempTable("table")
    hc.sql("select * from table").show()
    hc.sql("insert overwrite table db_hivetest."+tablename+" select * from table")

  }
  //写入mac地址每周出现次数结果
  def eachweekeachmaccountdatasavetoHive(sc:SparkContext, data:RDD[(String,String,Int)], databasename:String, tablename:String): Unit ={
    val hc = new HiveContext(sc)
    hc.sql("use "+databasename)
    hc.sql("create table "+tablename+"(mac string,weektime string,countnum int) row format delimited fields terminated by '/t'")
    import hc.implicits._
    val dataDF=data.toDF()
    dataDF.registerTempTable("t5")
    hc.sql("select * from t5").show()
    hc.sql("insert into table db_hivetest."+tablename+" select * from t5")
  }
  //写入每周常客数量结果
  def regularuserCountdatasavetoHive(sc:SparkContext, data:RDD[(String,Int,Long)], databasename:String, tablename:String): Unit ={
    val hc = new HiveContext(sc)
    hc.sql("use "+databasename)
    hc.sql("create table "+tablename+"(weektime string,regularuserflag int,countnum double) row format delimited fields terminated by '/t'")
    import hc.implicits._
    val dataDF=data.toDF()
    dataDF.registerTempTable("t4")
    hc.sql("select * from t4").show()
    hc.sql("insert into table db_hivetest."+tablename+" select * from t4")
  }
  //写入每个mac的来访周期结果
  def timelagtoeachmacdatasavetoHive(sc:SparkContext, data:RDD[(String,String)], databasename:String, tablename:String): Unit ={
    val hc = new HiveContext(sc)
    hc.sql("use "+databasename)
    hc.sql("create table "+tablename+"(mac string,timelag string) row format delimited fields terminated by '/t'")
    import hc.implicits._
    val dataDF=data.toDF()
    dataDF.registerTempTable("t7")
    hc.sql("select * from t7").show()
    hc.sql("insert into table db_hivetest."+tablename+" select * from t7")
  }
  //写入手机品牌结果
  def brandcountdatasavetoHive(sc:SparkContext, data:RDD[(String,Long)], databasename:String, tablename:String): Unit ={
    val hc = new HiveContext(sc)
    hc.sql("use "+databasename)
    hc.sql("create table "+tablename+"(brandname string,countnum double) row format delimited fields terminated by '/t'")
    import hc.implicits._
    val dataDF=data.toDF()
    dataDF.registerTempTable("t8")
    hc.sql("select * from t8").show()
    hc.sql("insert into table db_hivetest."+tablename+" select * from t8")
  }
}
