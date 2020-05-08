package com.SparkDefinitiveScala.Chapter.one

object Dataframes_Tutorial_1 extends App with Context {
  val dfRange = spark.range(1000).toDF("Number")
  val evenNum = dfRange.where("Number % 2 =0")
  println("The count of all even values are "+evenNum.count())
  val dfFlightData = spark.read
    .option("header",value = true)
    .option("inferSchema",value = true)
    .csv("D:\\Study_Materials\\spark-definitive-scala\\src\\main\\resources\\2015-summary.csv")
  val take5 = dfFlightData.take(5)
  println(s"Result od Dataframe.take(5) ${take5.mkString(",")}")
  dfFlightData.show(10)
  dfFlightData.sort("count").explain()
}
