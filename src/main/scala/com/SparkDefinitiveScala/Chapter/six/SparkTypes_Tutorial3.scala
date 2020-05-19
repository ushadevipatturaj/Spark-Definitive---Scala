package com.SparkDefinitiveScala.Chapter.six
import com.SparkDefinitiveScala.Chapter.one.Context
import org.apache.spark.sql.functions._
object SparkTypes_Tutorial3 extends App with Context{
  val dfCSV = spark.read
    .option("header",value = true)
    .option("inferSchema", value = true)
    .csv("D:\\Study_Materials\\spark-definitive-scala\\src\\main\\resources\\2010-12-01.csv")
  import spark.implicits._
  dfCSV.orderBy($"Description".asc_nulls_first).show(10,truncate = false)
  dfCSV.orderBy($"Description".desc_nulls_last).show(10,truncate = false)
}
