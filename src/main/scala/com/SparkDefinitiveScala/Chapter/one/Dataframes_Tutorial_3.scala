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
  dfRetail.selectExpr("CustomerID","(Quantity * UnitPrice) as total_cost","InvoiceDate")
    .groupBy(col("CustomerID"),window(col("InvoiceDate"),"1 day"))
    .sum("total_cost").orderBy(desc("sum(total_cost)")).show(5,truncate = false)


}
