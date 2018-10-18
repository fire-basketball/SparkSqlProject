package com.ligh.log

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  *  访问日志转换(输入转换成输出)工具类
  *
  *  输入: 访问时间,访问的URL,耗费的流量,访问IP地址信息
  *
  *  输出: URL, cmsType(video/article),cmsId(编号),流量,Ip,城市信息,访问时间,天
  */
object AccessConvertUtil {

  val struct = StructType(
    Array(
      StructField("url",StringType,nullable = true),
      StructField("cmsType",StringType,nullable = true),
      StructField("cmsId",StringType,nullable = true),
      StructField("traffic",StringType,nullable = true),
      StructField("ip",StringType,nullable = true),
      StructField("city",StringType,nullable = true),
      StructField("time",StringType,nullable = true),
      StructField("day",StringType,nullable = true)
    )
  )

  /**
    *  根据输入的每一行信息转换成输出的样式
    * @param log  输入的每一行记录信息
    */
  def parseLog(log:String)={

    try{
      val splits = log.split("\t")

      val url = splits(1)
      val traffic = splits(2)
      val ip = splits(3)

      val domain = "http://blog.fens.me/"
      val cms = url.substring(url.indexOf(domain) + domain.length)
      val cmsTypeId = cms.split("/")

      var cmsType = ""
      var cmsId = ""
      if(cmsTypeId.length > 1) {
        cmsType = cmsTypeId(0)
        cmsId = cmsTypeId(1)
      }

      val city = ""
      val time = splits(0)
      val day = time.substring(0,10).replaceAll("-","")

      //这个row里面的字段要和struct中的字段对应上
      Row(url, cmsType, cmsId, traffic, ip, city, time, day)
    }catch {
      case e:Exception => Row("","","","","","","","")
    }
  }
}
