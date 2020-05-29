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
  //filtering the dataset using a function
  def checkDomestic(flightRow :flight_data):Boolean={
    flightRow.ORIGIN_COUNTRY_NAME == flightRow.DEST_COUNTRY_NAME
  }
  def checkIndia(flightRow :flight_data):Boolean={
    (flightRow.ORIGIN_COUNTRY_NAME == "India") | ( flightRow.DEST_COUNTRY_NAME == "India")
  }

  flightsDataset.filter(row => checkDomestic(row)).show(10)
  flightsDataset.filter(row =>checkIndia(row)).show(10)

  //Mapping
  flightsDataset.take(20).map(flight => "The New "+flight.DEST_COUNTRY_NAME)

  //another case class to join with flight
  case class randomFlight(count:BigInt,randomvalue:BigInt)
  val randomFlightDataset = spark.range(100).map(row => (row,scala.util.Random.nextLong)).withColumnRenamed("_1","count")
    .withColumnRenamed("_2","randomvalue").as[randomFlight]
  randomFlightDataset.show(10)

  randomFlightDataset.join(flightsDataset,Seq("count")).show(10)
}
