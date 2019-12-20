package Mysql

import java.sql.{Connection, DriverManager}

object MysqlConnection {
  // 访问本地MySQL服务器，通过3306端口访问mysql数据库
  val url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf-8&useSSL=false"
  //驱动名称
  val driver = "com.mysql.jdbc.Driver"

  //用户名
  val username = "root"
  //密码
  val password = "root"
  def getconnection() :Connection={
    //初始化数据连接
    //注册Driver
    Class.forName(driver)
    //得到连接
    var connection: Connection = DriverManager.getConnection(url, username, password)
    if (connection.isClosed) {
      println("mysql sonnect is closed!")
    } else {
      println("mysql connect successfuly")
    }
    connection
  }

}
