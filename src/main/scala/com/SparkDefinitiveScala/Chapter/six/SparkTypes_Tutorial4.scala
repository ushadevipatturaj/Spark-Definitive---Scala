package com.SparkDefinitiveScala.Chapter.six
import com.SparkDefinitiveScala.Chapter.one.Context

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
object SparkTypes_Tutorial4 extends App with Context{
  val dfCSV = spark.read
    .option("header",value = true)
    .option("inferSchema", value = true)
    .csv("D:\\Study_Materials\\spark-definitive-scala\\src\\main\\resources\\2010-12-01.csv")
  import spark.implicits._

  //Working with Json files
  val jsonString = """ '{"MyJasonKey":{"MyJsonValue1":[1,2,3],"MyJsonValue2":[3,4,5]}}' """
  val dfJson = spark.range(1).select(expr(jsonString).as("JsonString"))
  dfJson.select(get_json_object($"JsonString","$.MyJasonKey.MyJsonValue1[2]"),
    json_tuple($"JsonString","MyJasonKey")).show(truncate = false)

  //to_json
  dfCSV.selectExpr("(InvoiceNo,Description) as Json_Type")
    .select(to_json($"Json_Type")).show(5,truncate = false)

  //from_json
  val parseSchema =  StructType(Array(StructField("InvoiceNo",StringType,nullable = true),
    StructField("Description",StringType,nullable = true)))

  dfCSV.selectExpr("(InvoiceNo,Description) as Json_Type")
    .select(to_json($"Json_Type").as("JsonValue"))
    .select(from_json($"JsonValue",parseSchema)).show(5,truncate = false)

  //udf
  def pow3(value :Double):Double={
    value*value*value
  }
  val udfFunction = udf(pow3(_:Double):Double)
  dfCSV.select(udfFunction($"Quantity")).show(10,truncate = false)

  //registering udf
  spark.udf.register("power3",pow3(_:Double):Double)
  dfCSV.selectExpr("power3(Quantity)").show(10,truncate = false)
}
