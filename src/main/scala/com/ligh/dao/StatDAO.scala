package com.ligh.dao

import java.sql.{Connection, PreparedStatement}

import com.ligh.entity.{DayCityVideoAccessStat, DayTrafficVideoAccessStat, DayVideoAccessStat}
import com.ligh.util.MysqlUtils

import scala.collection.mutable.ListBuffer

/**
  * 各个维度统计的DAO
  */
object StatDAO {

  /**
    *  批量保存DayVideoAccessStat到数据库
    * @param list
    */
    def insertDayVideoAccessTopN(list: ListBuffer[DayVideoAccessStat]): Unit ={

      var connection:Connection = null
      var pstmt:PreparedStatement = null

   //   connection.setAutoCommit(false) //设置手动进行提交  因为默认的是自动提交
      try{
        connection = MysqlUtils.getConnection()

        connection.setAutoCommit(false) //设置手动提交

        val sql = "insert into day_video_access_topn_stat(day,cms_id,times) values (?,?,?) "
        pstmt = connection.prepareStatement(sql)

        for (ele <- list) {
          pstmt.setString(1, ele.day)
          pstmt.setString(2, ele.cmsId)
          pstmt.setLong(3, ele.times)

          pstmt.addBatch()
        }

        pstmt.executeBatch() // 执行批量处理
        connection.commit() //手工提交
      }catch {
        case e:Exception => e.printStackTrace()
      }finally {
        MysqlUtils.release(connection,pstmt)
      }
    }

  /**
    *  批量保存DayCityVideoAccessStat到数据库
    * @param list
    */
  def insertDayCityVideoAccessTopN(list: ListBuffer[DayCityVideoAccessStat]): Unit ={

    var connection:Connection = null
    var pstmt:PreparedStatement = null

    //   connection.setAutoCommit(false) //设置手动进行提交  因为默认的是自动提交
    try{
      connection = MysqlUtils.getConnection()

      connection.setAutoCommit(false) //设置手动提交

      val sql = "insert into day_video_city_access_topn_stat(day,cms_id,city,times,times_rank) values (?,?,?,?,?)"
      pstmt = connection.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setString(2, ele.cmsId)
        pstmt.setString(3,ele.city)
        pstmt.setLong(4, ele.times)
        pstmt.setInt(5,ele.timesRank)
        pstmt.addBatch()
      }

      pstmt.executeBatch() // 执行批量处理
      connection.commit() //手工提交
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      MysqlUtils.release(connection,pstmt)
    }
  }
  /**
    *  批量保存DayTrafficVideoAccessStat到数据库
    * @param list
    */
  def insertDayTrafficVideoAccessTopN(list: ListBuffer[DayTrafficVideoAccessStat]): Unit ={

    var connection:Connection = null
    var pstmt:PreparedStatement = null

    //   connection.setAutoCommit(false) //设置手动进行提交  因为默认的是自动提交
    try{
      connection = MysqlUtils.getConnection()

      connection.setAutoCommit(false) //设置手动提交

      val sql = "insert into day_video_traffic_access_topn_stat(day,cms_id,traffics) values (?,?,?)"
      pstmt = connection.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setString(2, ele.cmsId)
        pstmt.setDouble(3,ele.traffics)
        pstmt.addBatch()
      }
      pstmt.executeBatch() // 执行批量处理
      connection.commit() //手工提交
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      MysqlUtils.release(connection,pstmt)
    }
  }

  /**
    *  删除指定日期的数据
    * @param day
    */
  def deleteData(day:String): Unit ={

    val tables = Array("day_video_access_topn_stat","day_video_city_access_topn_stat","day_video_traffic_access_topn_stat")

    var connection:Connection = null
    var pstmt:PreparedStatement = null

    try{
      connection = MysqlUtils.getConnection()

      for(table <- tables){
        val deleteSQL = s"delete from $table where day = ?"
        pstmt = connection.prepareStatement(deleteSQL)
        pstmt.executeUpdate()
      }

    }catch {
      case e:Exception => e.printStackTrace()
    }
  }
}
