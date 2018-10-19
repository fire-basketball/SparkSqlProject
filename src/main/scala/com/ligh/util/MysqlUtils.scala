package com.ligh.util

import java.sql.{DriverManager, PreparedStatement}

import com.mysql.jdbc.Connection

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

  /**
    *  数据库关闭之后,释放资源
    * @param connoection
    * @param pstms
    */
  def release(connoection:Connection,pstms:PreparedStatement): Unit ={
    try{
      if(pstms != null){
        pstms.close()
      }
    }catch {
      case e :Exception => e.printStackTrace()
    }finally {
      if(connoection != null){
        connoection.close()
      }
    }


  }

  /**
    *  测试连接是否成功
    * @param args
    */
  def main(args: Array[String]) {

    println(getConnection())

  }

}
