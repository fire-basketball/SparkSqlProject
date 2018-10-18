package com.ligh.log

import org.apache.spark.sql.SparkSession

/**
  *  使用spark完成数据的清洗操作
  */
object SparkStatCleanJob {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]").getOrCreate()

    val accessRDD = spark.sparkContext.textFile("file:///Users/fish/Desktop/output/access.log")
    accessRDD.take(100).foreach(println)



    spark.stop()

  }
}
