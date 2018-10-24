package com.ligh.log

import com.ligh.dao.StatDAO
import com.ligh.entity.DayVideoAccessStat
import org.apache.spark.sql.{DataFrame, SparkSession}

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

    videoAccessTopNStat(spark,accsessDF)

    spark.stop
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
