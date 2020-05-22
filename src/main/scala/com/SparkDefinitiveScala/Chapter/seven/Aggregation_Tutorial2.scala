package com.SparkDefinitiveScala.Chapter.seven
import com.SparkDefinitiveScala.Chapter.one.Context
import org.apache.spark.sql.expressions.Window
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

  //Window
  val windowSpec = Window.partitionBy("CustomerID","InvoiceDate").orderBy(desc("Quantity"))
    .rowsBetween(Window.unboundedPreceding,Window.currentRow)
  val maxPurchase = max("Quantity").over(windowSpec)
  val rankval = rank().over(windowSpec)
  val denseRank = dense_rank().over(windowSpec)
  import spark.implicits._
  dfCSV.select($"Quantity",$"InvoiceDate",$"CustomerID",maxPurchase.as("MaxPurchase"),rankval.as("Rank"),
    denseRank.as("DenseRank")).show(truncate = false)

  //Grouping Sets only on SQL
  dfCSV.createOrReplaceTempView("DFView")
  spark.sql("""
    select CustomerID,StockCode,sum(Quantity) from DFView Group by CustomerID,StockCode Grouping Sets(CustomerID,StockCode)
  order by CustomerID asc nulls last,StockCode asc nulls last""")
    .show(25, truncate = false)

  //rollups

  dfCSV.rollup("InvoiceDate", "Country").agg(sum("Quantity"))
    .selectExpr("InvoiceDate", "Country", "`sum(Quantity)` as total_quantity") .orderBy("InvoiceDate")
    .show(10, truncate = false)

  //cube
  dfCSV.cube("InvoiceDate", "Country").agg(sum("Quantity"))
    .selectExpr("InvoiceDate", "Country", "`sum(Quantity)` as total_quantity") .orderBy("InvoiceDate")
    .show(10, truncate = false)
}
