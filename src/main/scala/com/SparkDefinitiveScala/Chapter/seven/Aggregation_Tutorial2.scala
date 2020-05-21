package com.SparkDefinitiveScala.Chapter.seven
import com.SparkDefinitiveScala.Chapter.one.Context
import org.apache.spark.sql.functions._
object Aggregation_Tutorial2 extends App with Context {
  val dfCSV = spark.read
    .option("header",value = true)
    .option("inferSchema", value = true)
    .csv("D:\\Study_Materials\\spark-definitive-scala\\src\\main\\resources\\2010-12-01.csv")
  //Grouping
  //GroupBy

  dfCSV.groupBy("InvoiceNo","CustomerID").count().show()

  //Grouping with Expressions
  dfCSV.groupBy("CustomerID").agg(count("InvoiceNo"),expr("sum(Quantity)")).show(10,truncate = false)

  //Grouping with Map
  dfCSV.groupBy("CustomerID").agg("InvoiceNo" -> "count","Quantity" -> "sum","Quantity" -> "avg")
    .show(10,truncate = false)
}
