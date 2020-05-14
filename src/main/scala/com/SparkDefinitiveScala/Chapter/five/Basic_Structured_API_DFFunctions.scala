package com.SparkDefinitiveScala.Chapter.five

import com.SparkDefinitiveScala.Chapter.one.Context
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

object Basic_Structured_API_DFFunctions extends App with Context{
  val jsonSchema = spark.read.format("json")
    .load("D:\\Study_Materials\\spark-definitive-scala\\src\\main\\resources\\2015-summary.json").schema

  val dfJson = spark.read.format("json")
    .load("D:\\Study_Materials\\spark-definitive-scala\\src\\main\\resources\\2015-summary.json")
  //union of two dataframes
  val newRow1 = Row("Chennai","Tuticorin",1L)
  val newRow2 = Row("Bangalore","Chennai",1L)
  val rows = Seq(newRow1,newRow2)
  val parallelRows = spark.sparkContext.parallelize(rows)
  val dfNewRows = spark.createDataFrame(parallelRows,jsonSchema)

  val newJsonDF = dfNewRows.union(dfJson)
  newJsonDF.show(10)

  //sorting the columns of dataframes
  dfJson.sort("count").show(5)
  dfJson.orderBy("ORIGIN_COUNTRY_NAME","DEST_COUNTRY_NAME").show(5)
  dfJson.orderBy(desc("ORIGIN_COUNTRY_NAME"),desc("DEST_COUNTRY_NAME")).show(5)
  dfJson.orderBy(expr("ORIGIN_COUNTRY_NAME desc_nulls_first")).show(5)
  dfJson.orderBy(col("ORIGIN_COUNTRY_NAME").desc_nulls_last,col("DEST_COUNTRY_NAME").desc).show()
  val dfJson_sorted = spark.read.format("json")
    .load("D:\\Study_Materials\\spark-definitive-scala\\src\\main\\resources\\2015-summary.json").sortWithinPartitions("ORIGIN_COUNTRY_NAME")
  dfJson_sorted.show(5)

  //limit
  dfJson.limit(10).show()

}
