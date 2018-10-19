package com.ligh.log

import com.ligh.util.AccessConvertUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

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

    accessDF.printSchema()
  //  accessDF.filter(accessDF.col("url") isNotNull).show(100)
//    accessDF.show(100)
  // coalesce 是做优化操作，把生成的文件放在一个文件中（就是合并文件）  mode方法是为了文件目录存在时不会报文件已存在的错误
    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).partitionBy("day")
      .save("/Users/fish/Desktop/output/clean")
    //生成的就是parquet类型的文件，也就是做统计的源文件
    spark.stop

  }

}
