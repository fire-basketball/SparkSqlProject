package com.ligh.log

import org.apache.spark.sql.SparkSession

/**
  *  使用spark完成数据的清洗操作
  */
object SparkStatCleanJob {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]").getOrCreate()

    val accessRDD = spark.sparkContext.textFile("/Users/fish/Desktop/output/access.log")
 //   accessRDD.take(100).foreach(println)


    /**
      * StructType(header_clean.split(",").map(fieldName ⇒StructField(fieldName, StringType, true)))
        var contentRdd = contentNoHeader.map(k => k.split(",")).map(
        p => {
         val ppp = p.map( x => x.replace("\"", "").trim)
       Row(ppp(0),ppp(1),ppp(2))
      })
      */


    val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)),

      AccessConvertUtil.struct)

    accessDF.show(20)

    accessDF.printSchema()
    accessDF.show()

    accessDF.show(false)

    spark.stop

  }

}
