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
}
