package com.SparkDefinitiveScala.Chapter.seven
import com.SparkDefinitiveScala.Chapter.one.Context
import org.apache.spark.sql.functions._
object Aggregation_Tutorial1 extends App with Context {
  val dfCSV = spark.read
    .option("header",value = true)
    .option("inferSchema", value = true)
    .csv("D:\\Study_Materials\\spark-definitive-scala\\src\\main\\resources\\2010-12-01.csv")
  import spark.implicits._
  //count,countDistinct,approximate_Count
  dfCSV.select(count("StockCode"),countDistinct("StockCode")
    ,approx_count_distinct("StockCode",0.1)).show(10,truncate = true)

  //sum,first,last,min,max
  dfCSV.select(sum("Quantity"),min("Quantity"),max("Quantity")
  ,first("Description"),last("Description")).show(10,truncate = true)
  val firstRow = dfCSV.first()
  println(firstRow.mkString(","))

  dfCSV.select(sumDistinct("Quantity"),avg("Quantity"),variance("Quantity")
    ,var_pop("Quantity"),var_samp("Quantity"),stddev("Quantity")
    ,stddev_pop("Quantity"),stddev_samp("Quantity")).show(truncate = false)
}
