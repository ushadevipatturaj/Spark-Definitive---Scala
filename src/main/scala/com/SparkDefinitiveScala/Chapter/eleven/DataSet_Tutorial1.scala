package com.SparkDefinitiveScala.Chapter.eleven

import com.SparkDefinitiveScala.Chapter.nine.Context
import org.apache.spark.sql.Dataset

object DataSet_Tutorial1 extends App with Context{
  import spark.implicits._
  val parquetFile = spark.read.format("parquet")
    .load("D:\\Study_Materials\\spark-definitive-scala\\src\\main\\resources\\2015-summary-fromcode.csv\\")
  case class flight_data(DEST_COUNTRY_NAME : String ,ORIGIN_COUNTRY_NAME : String,count : BigInt)
  val flightsDataset:Dataset[flight_data] = parquetFile.as[flight_data]
  flightsDataset.take(10).foreach(row => println(row .DEST_COUNTRY_NAME,row.ORIGIN_COUNTRY_NAME,row.count))
}
