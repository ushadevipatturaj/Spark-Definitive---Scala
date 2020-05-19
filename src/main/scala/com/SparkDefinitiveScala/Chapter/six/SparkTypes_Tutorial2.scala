package com.SparkDefinitiveScala.Chapter.six
import com.SparkDefinitiveScala.Chapter.one.Context
import org.apache.spark.sql.functions._

object SparkTypes_Tutorial2 extends App with Context {
  val dfCSV = spark.read
    .option("header",value = true)
    .option("inferSchema", value = true)
    .csv("D:\\Study_Materials\\spark-definitive-scala\\src\\main\\resources\\2010-12-01.csv")

  //getting the columns to get validated with random seq list
  val seqColours = Seq("black", "white", "red", "green", "blue")
  val sampleColours = seqColours.map(colour => {
      col("Description").contains(colour.toUpperCase()).alias(s"is_$colour")
    }):+expr("*")
  dfCSV.select(sampleColours:_*).where(col("is_red") or col("is_white"))
    .select("Description").show(10,truncate = false)

  //Date and Timestamp functions
  import spark.implicits._
  val dfDate_Time = spark.range(100)
    .withColumn("Today",current_date())
    .withColumn("Now",current_timestamp())
  dfDate_Time.show(10,truncate = false)
  dfDate_Time.printSchema()

  //date_add and date_sub
  dfDate_Time.select(date_add($"Today",3),date_sub($"Today",3)).show(5,truncate = false)

  //datediff and months_between
  dfDate_Time.withColumn("OldDate",date_add($"Today",3))
    .withColumn("NewDate",date_sub($"Today",3))
    .select(datediff($"OldDate",$"NewDate"),months_between($"OldDate",$"NewDate")).show(1)

  //to_date and date_format and to_timestamp
  spark.range(5).withColumn("DateConverted" , to_date(lit("20-04-2020"),"dd-MM-yyyy"))
    .withColumn("diffDateFormat",date_format($"DateConverted","yyyyMMdd")).show(5)
  dfCSV.select(to_timestamp($"InvoiceDate","yyyy-MM-dd HH:mm:ss"))
    .withColumn("LiteralTimestamp",to_timestamp(lit("2010-12-07"),"yyyy-MM-dd"))
    .filter($"InvoiceDate">lit("2010-12-01 08:30:00")).show(5,truncate = false)

  //Handling Null values
  //coalesce gives the non null rows
  dfCSV.select(coalesce($"Description"),coalesce($"CustomerID")).show(5,truncate = false)
  //ifnull,nullif,nvl,nvl2
  val countNotNull = dfCSV.na.drop().count()
  val countNull = dfCSV.count()
  println(s"Count of not null rows and null rows $countNotNull $countNull")

}
