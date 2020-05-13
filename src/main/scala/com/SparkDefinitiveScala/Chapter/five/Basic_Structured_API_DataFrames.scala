package com.SparkDefinitiveScala.Chapter.five
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructType
import com.SparkDefinitiveScala.Chapter.one.Context
import org.apache.spark.sql.Row

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

  //creating a new row manually
  val myRow = Row("Hello",1,true,null)
  println(myRow.get(0))
  println(myRow.getString(0))
  println(myRow.getInt(1))
  println(myRow.getAs[Boolean](2))
  println(myRow.get(3).asInstanceOf[String])
  println(s"Checking the datatype of a first value is string?: ${myRow.get(0).isInstanceOf[String]}")

  val dfSQLView: Unit = dfJson.createOrReplaceTempView("JsonView")
  //creating dataframes using RDD from manual rows

  val manualSchema_Row = StructType(Array(StructField("Name",StringType,nullable = true),
    StructField("Id",IntegerType,nullable = true),
    StructField("Day_Scholar",BooleanType,nullable = true)))

  val newRow = Seq(Row("Usha",122,true))
  val newRDD = spark.sparkContext.parallelize(newRow)
  val dffromRDD = spark.createDataFrame(newRDD,manualSchema_Row)
  dffromRDD.show(10)

}
