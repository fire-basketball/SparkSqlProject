package com.ligh.util

import com.ggstar.util.ip.IpHelper

/**
  * 解析ip获得所在城市信息
  */
object IpUtils {

    def getCity(ip:String) = {
      IpHelper.findRegionByIp(ip)
    }

  def main(args: Array[String]){

      val city = getCity("218.75.35.226")

    println(city)
  }
}
