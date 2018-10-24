package com.ligh.log

import com.ligh.dao.StatDAO
import com.ligh.entity.{DayCityVideoAccessStat, DayTrafficVideoAccessStat, DayVideoAccessStat}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
  * TopN统计Spark作业
  *
  */

object TopNStatJob {


  def main(args: Array[String]){

    val spark = SparkSession.builder()
      .appName("TopNStatJob")
      .master("local[2]")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled",false)  //设置的目的是因为时间day，系统默认推到出的类型是Integer类型，但是我想要的是String类型的，因此加上
      .getOrCreate()

    val accsessDF = spark.read.format("parquet").load("/Users/fish/Desktop/output/clean")

//    accsessDF.printSchema()
//    accsessDF.show(false)

    /**
      *  统计最受欢迎的TopN课程
      *
      */
   // videoAccessTopNStat(spark,accsessDF)

    /**
      * 按照地市进行统计TopN课程
      */
   // cityAccessTopNStat(spark,accsessDF)

    /**
      *  按照流量进行统计
      */
      videoTrafficsTopNStat(spark,accsessDF)

    spark.stop
  }

  /**
    *  按照流量进行统计
    * @param spark
    * @param accessDF
    */
  def videoTrafficsTopNStat(spark: SparkSession,accessDF:DataFrame): Unit ={
    //使用DataFrame进行统计
    import spark.implicits._
    val trafficAccessTopNDF =  accessDF.filter($"day" === "20130919" && $"cmsType" === "js")
      .groupBy("day","cmsId").agg(sum("traffic").as("traffics"))
      .orderBy($"traffics".desc)
      //.show(false)   存储数据库的时候要注释掉
    /**
      *  按照流量进行数据库的存储
      */
    try{
      trafficAccessTopNDF.foreachPartition(partitionOfRecode =>{
        var list = new ListBuffer[DayTrafficVideoAccessStat]

        partitionOfRecode.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[String]("cmsId")
          val traffics = info.getAs[Double]("traffics")
          list.append(DayTrafficVideoAccessStat(day,cmsId,traffics))
        })
        StatDAO.insertDayTrafficVideoAccessTopN(list)

      })

    }catch {
      case e:Exception => e.printStackTrace()
    }

  }

  /**
    *  地市统计最受欢迎的课程
    * @param spark
    * @param accessDF
    */
  def cityAccessTopNStat(spark: SparkSession,accessDF:DataFrame): Unit ={
    //使用DataFrame进行统计
        import spark.implicits._
        val cityAccessTopNDF =  accessDF.filter($"day" === "20130919" && $"cmsType" === "js")
          .groupBy("day","city","cmsId").agg(count("cmsId").as("times"))
    // window函数在Spark SQL中的使用

    val top3DF = cityAccessTopNDF.select(
      cityAccessTopNDF("day"),
      cityAccessTopNDF("cmsId"),
      cityAccessTopNDF("city"),
      cityAccessTopNDF("times"),
      row_number().over(Window.partitionBy(cityAccessTopNDF("city"))
        .orderBy(cityAccessTopNDF("times").desc)
      ).as("times_rank")
    ).filter("times_rank <= 3") //.show(false)  //只统计一个城市前三个最受欢迎

    /**
      *  将统计结果放在数据库中
      */
    try{
      top3DF.foreachPartition(partitionOfRecode =>{
        var list = new ListBuffer[DayCityVideoAccessStat]

        partitionOfRecode.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[String]("cmsId")
          val city = info.getAs[String]("city")
          val times = info.getAs[Long]("times")
          val timesRank = info.getAs[Int]("times_rank")
          list.append(DayCityVideoAccessStat(day,cmsId,city,times,timesRank))
        })
        StatDAO.insertDayCityVideoAccessTopN(list)

      })

    }catch {
      case e:Exception => e.printStackTrace()
    }

  }


  /**
    * 最受欢迎的TopN课程
    * @param spark
    * @param accessDF
    */
  def videoAccessTopNStat(spark: SparkSession,accessDF:DataFrame):Unit={

    //使用DataFrame进行统计
//    import spark.implicits._
//    val videoAccessTopNDF =  accessDF.filter($"day" === "20130919" && $"cmsType" === "js")
//      .groupBy("day","cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)

    //使用spark sql进行统计
    accessDF.createOrReplaceTempView("access_logs")
    val videoAccessTopNDF =  spark.sql("select day,cmsId,count(1) as times from access_logs where day='20130919' and cmsType='js' group by day,cmsId order by times desc")

//   videoAccessTopNDF.show(1000)
    /**
      *  将统计结果放在数据库中
      */
    try{
        videoAccessTopNDF.foreachPartition(partitionOfRecode =>{
        var list = new ListBuffer[DayVideoAccessStat]

        partitionOfRecode.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[String]("cmsId")
          val times = info.getAs[Long]("times")
          list.append(DayVideoAccessStat(day,cmsId,times))
        })
        StatDAO.insertDayVideoAccessTopN(list)

      })
    }catch {
      case e:Exception => e.printStackTrace()
    }

  }

}
