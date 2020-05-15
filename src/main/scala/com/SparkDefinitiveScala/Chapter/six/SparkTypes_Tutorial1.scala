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
  //checking whether the value is available
  dfCSV.select("InvoiceNo","StockCode","Quantity","InvoiceDate","UnitPrice").where($"InvoiceNo".equalTo(536365)).show()
  dfCSV.select("InvoiceNo","StockCode","Quantity","InvoiceDate","UnitPrice").where($"InvoiceNo" ===536365).show()
  dfCSV.where("InvoiceNo = 536365").show()

  dfCSV.where($"StockCode".contains("DOT").or($"InvoiceNo" ===536365).and($"CustomerID".isNotNull)).show()
  val stockCode = $"StockCode".contains("DOT")
  val unitPrice = $"UnitPrice" > 600
  dfCSV.withColumn("isExpensive",stockCode and unitPrice)
    .select("InvoiceNo","StockCode","Quantity","InvoiceDate","UnitPrice","isExpensive").where($"isExpensive").show()
  dfCSV.withColumn("isExpensive",not($"UnitPrice" >= 20)).filter($"isExpensive").show(5)

  //Numbers related functions
  dfCSV.select(col("UnitPrice"),col("Quantity"),round(pow($"Quantity" * $"UnitPrice",2 ),2).alias("total_Price_withRound2Dec")
    ,round($"UnitPrice").alias("total_Price_withRound")
    ,bround($"UnitPrice").alias("total_Price_withBRound")).show(10)
  dfCSV.select(corr($"Quantity" , $"UnitPrice")).show()
  dfCSV.describe().show()
  val covariance = dfCSV.stat.cov("Quantity" , "UnitPrice")
  println(s"covariance between  Quantity UnitPrice is $covariance")
  val approxQuantile = dfCSV.stat.approxQuantile("Quantity" ,Array(0.5),0.05)
  println(s"approximate Quantile of UnitPrice is ${approxQuantile.mkString(",")}")

}
