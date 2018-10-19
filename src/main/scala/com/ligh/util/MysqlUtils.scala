package com.ligh.util

import java.sql.DriverManager

/**
  *  Mysql操作工具类
  */

object MysqlUtils {

  /**
    * 获取数据库的连接
    * @return
    */
  def getConnection() = {

    DriverManager.getConnection("jdbc:mysql://47.100.176.47:3306/imooc_project?user=root&password=123456")
  }

  def main(args: Array[String]) {

    println(getConnection())

  }

}
