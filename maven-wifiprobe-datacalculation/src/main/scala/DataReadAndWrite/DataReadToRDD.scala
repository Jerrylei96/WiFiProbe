package DataReadAndWrite

import HBase.HBaseConnection
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext

object DataReadToRDD {

  def gethbaseRDD(sc:SparkContext):RDD[(String,String)]={
    val inputTable="WIFIProbe"
    //配置输入的表为testTable
    val inputHbaseConf=HBaseConnection.getConf()
    inputHbaseConf.set(TableInputFormat.INPUT_TABLE, inputTable)

    //从hbase中读取为RDD
    val hbaseRDD=sc.newAPIHadoopRDD(inputHbaseConf,classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])
    val count=hbaseRDD.count()
    println("test RDD count:"+count)
    //hbaseRDD.cache()
    //取出字段：时间和源mac地址
    val cleanRDD= hbaseRDD.map({
      case (_, result) =>
        val row: String = Bytes.toString(result.getRow)
        val time: String = row.split("##")(0)
        val resMac:String=row.split("##")(1)
        (resMac,time)
    })
    val myRDD=cleanRDD.filter(x=>x._1.contains(":"))
    myRDD
  }

  def gethiveRDD(hiveContext:HiveContext):RDD[(String,String)]= {
    hiveContext.sql("use db_hivetest")
    val hivedf = hiveContext.sql("select * from wifiprobe").rdd
    hivedf.map(row=>{
      val resMac=row.get(0).toString
      val time=row.get(1).toString
      (resMac,time)
    })

  }


  def gettestRDD(sc:SparkContext):RDD[(String,String)]= {
    val arr = Array(("10:32:7E:11:22:33", "2019-02-02 07:20:00"),
      ("10:32:7E:11:22:33", "2019-02-02 08:20:00"),
      ("10:32:7E:11:22:33", "2019-02-02 07:22:00"),
      ("18:02:AE:11:22:33", "2019-02-02 07:25:00"),
      ("A4:45:19:11:22:33", "2019-02-02 09:20:00"),
      ("10:32:7E:11:22:33", "2019-02-02 10:20:00"),
      ("10:32:7E:11:22:33", "2019-02-03 10:20:00"),
      ("44:D7:91:11:22:11", "2019-02-03 11:20:00"),
      ("44:D7:91:11:22:11", "2019-02-04 10:20:00"),
      ("44:D7:91:11:22:11", "2019-02-06 10:20:00"),
      ("44:D7:91:11:22:11", "2019-02-07 10:20:00"),
      ("44:D7:91:11:22:11", "2019-02-08 10:20:00"),
      ("44:D7:91:11:22:11", "2019-02-12 10:20:00"),
      ("44:D7:91:11:22:11", "2019-02-12 11:20:00"),
      ("44:D7:91:11:22:11", "2019-02-12 12:40:00"),
      ("44:D7:91:11:22:11", "2019-02-16 10:20:00"),
      ("44:D7:91:11:22:11", "2019-02-15 10:20:00"),
      ("44:D7:91:11:22:11", "2019-02-14 10:20:00"),
      ("44:D7:91:11:22:11", "2019-02-14 11:20:00"),
      ("44:D7:91:11:22:11", "2019-02-15 11:20:00"),
      ("00:00:00:00:22:11", "2019-03-02 11:22:00"),
      ("00:00:00:00:22:11", "2019-03-01 11:22:00")
    )
    val testRDD = sc.parallelize(arr).sortBy(_._2)
    testRDD
  }





}
