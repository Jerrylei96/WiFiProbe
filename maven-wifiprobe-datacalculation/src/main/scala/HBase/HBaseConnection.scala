package HBase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

/**
  * 设置hBase连接配置
  */
object HBaseConnection {
  // 配置hBase的主节点ip
  val master: String = "192.168.1.2"
  // 配置HBase的zookeeper节点ip
  val zookeeper: String = "192.168.1.2,192.168.1.3,192.168.1.4"
  // 配置HBase的zookeeper连接端口，默认值为2181
  val port = "2181"

  def getConf(): Configuration = {
    val hbaseConf: Configuration = new Configuration
    hbaseConf.set("hbase.master", master)
    hbaseConf.set("hbase.zookeeper.quorum", zookeeper)
    hbaseConf.set("hbase.zookeeper.property.clientPort", port)
    hbaseConf
  }
  def getconnection(config:Configuration): Connection={
    val connection = ConnectionFactory.createConnection(config)
    if(connection.isClosed){
      println("HBase Connection is close!")
    }
    else {
    println("Hbase Connect successfully")
    }
    connection
  }
}
