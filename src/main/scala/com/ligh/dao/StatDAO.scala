package com.ligh.dao

import java.sql.{Connection, PreparedStatement}

import com.ligh.entity.DayVideoAccessStat
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
}
