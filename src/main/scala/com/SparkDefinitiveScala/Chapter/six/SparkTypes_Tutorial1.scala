package com.SparkDefinitiveScala.Chapter.six

import com.SparkDefinitiveScala.Chapter.one.Context
import org.apache.spark.sql.functions._


object SparkTypes_Tutorial1 extends App with Context{
  val dfCSV = spark.read
    .option("header",value = true)
    .option("inferSchema", value = true)
    .csv("D:\\Study_Materials\\spark-definitive-scala\\src\\main\\resources\\2010-12-01.csv")

  dfCSV.show(5)
  //converting to the spark types
  dfCSV.select(lit(5),lit("five"),lit(5.0)).show(2)
  import spark.implicits._
  dfCSV.select("InvoiceNo","StockCode","Quantity","InvoiceDate","UnitPrice").where($"InvoiceNo".equalTo(536365)).show()
}
