package com.SparkDefinitiveScala.Chapter.five
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructType
import com.SparkDefinitiveScala.Chapter.one.Context

object Basic_Structured_API_DataFrames extends App with Context{
  val jsonSchema = spark.read.format("json")
    .load("D:\\Study_Materials\\spark-definitive-scala\\src\\main\\resources\\2015-summary.json").schema
  println(jsonSchema.mkString(","))

  val manualSchema = StructType(Array(StructField("ORIGIN_COUNTRY_NAME",StringType,nullable = true),
    StructField("DEST_COUNTRY_NAME",StringType,nullable = true),
    StructField("count",LongType,nullable = false,Metadata.fromJson("{\"hello\":\"world\"}"))))

  val dfJson = spark.read
    .schema(manualSchema).format("json")
    .load("D:\\Study_Materials\\spark-definitive-scala\\src\\main\\resources\\2015-summary.json")
  dfJson.show(10)

  //getting all columns of the dataframe
  val dfColumns = dfJson.columns
  println(dfColumns.mkString(","))

  //getting the first row of a dataframe
  val dfFirstRow = dfJson.first()
  println(dfFirstRow)


}
