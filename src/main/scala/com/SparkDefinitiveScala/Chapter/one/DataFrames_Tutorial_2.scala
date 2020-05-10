package com.SparkDefinitiveScala.Chapter.one

object DataFrames_Tutorial_2 extends App with Context {
  case class flights(DEST_COUNTRY_NAME:String,ORIGIN_COUNTRY_NAME:String,count:Int)
  val dfFlight = spark.read
    .option("header",value = true)
    .option("inferSchema", value = true)
    .csv("D:\\Study_Materials\\spark-definitive-scala\\src\\main\\resources\\2015-summary.csv")
    .toDF()
  import spark.implicits._
  val dsFlight = dfFlight.as[flights]
  dsFlight.filter(row => row.DEST_COUNTRY_NAME != "Italy")
    .map(row => row)
    .show(5)
  dsFlight.filter(row => row.DEST_COUNTRY_NAME != "Italy")
    .map(row => flights(row.DEST_COUNTRY_NAME,row.ORIGIN_COUNTRY_NAME,row.count)).show(5)


}
