package com.SparkDefinitiveScala.Chapter.one
import org.apache.spark.sql.functions._
object Dataframes_Tutorial_3 extends App with Context {
  val dfRetail = spark.read
    .option("header",value = true)
    .option("inferSchema",value = true)
    .csv("D:\\Study_Materials\\spark-definitive-scala\\src\\main\\resources\\2010*.csv")

  dfRetail.show(10)
  val staticSchema = dfRetail.schema
  println(staticSchema.fields.mkString(","))
  //getting details of which customer makes most of the shopping at which time

  import spark.implicits._
  dfRetail.selectExpr("CustomerID","(Quantity * UnitPrice) as total_cost","InvoiceDate")
    .groupBy($"CustomerID",window($"InvoiceDate","1 day"))
    .sum("total_cost").orderBy(desc("sum(total_cost)")).show(5,truncate = false)

  //streaming of files - read stream
  val dfStream = spark.readStream
    .option("header",value = true)
    .option("maxFilesPerTrigger" ,1)
    .format("csv")
    .schema(staticSchema)
    .load("D:\\Study_Materials\\spark-definitive-scala\\src\\main\\resources\\2010*.csv")

  val customerPurchase = dfStream.selectExpr("CustomerID","(Quantity * UnitPrice) as total_cost ","InvoiceDate")
    .groupBy($"CustomerID",window($"InvoiceDate" ,"1 day"))
    .sum("total_cost").orderBy(desc("sum(total_cost)"))

  println(s"Is streaming in progress? ${dfStream.isStreaming}")

  //streaming of files - write stream
  val topPurchaseCustomer = customerPurchase.writeStream
    .format("console")
    .queryName("TopPurchases")
    .outputMode("complete")
    .start()



}
