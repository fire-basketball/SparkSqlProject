package com.ligh.log

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
      StructField("url",StringType),
      StructField("cmsType",StringType),
      StructField("cmsId",LongType),
      StructField("traffic",LongType),
      StructField("ip",StringType),
      StructField("city",StringType),
      StructField("time",StringType),
      StructField("day",StringType)
    )
  )

  /**
    *  根据输入的每一行信息转换成输出的样式
    * @param log  输入的每一行记录信息
    */
  def parseLog(log:String){

    val splits = log.split("\t")

    val url = splits(1)

    val traffic = splits(2)


  }
}
