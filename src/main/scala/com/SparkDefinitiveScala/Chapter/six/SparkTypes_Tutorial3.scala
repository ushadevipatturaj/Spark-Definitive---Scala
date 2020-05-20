package com.SparkDefinitiveScala.Chapter.six
import com.SparkDefinitiveScala.Chapter.one.Context
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
object SparkTypes_Tutorial3 extends App with Context{
  val dfCSV = spark.read
    .option("header",value = true)
    .option("inferSchema", value = true)
    .csv("D:\\Study_Materials\\spark-definitive-scala\\src\\main\\resources\\2010-12-01.csv")
  import spark.implicits._
  dfCSV.orderBy($"Description".asc_nulls_first).show(10,truncate = false)
  dfCSV.orderBy($"Description".desc_nulls_last).show(10,truncate = false)

  //complex types of spark
  val dfComplex = dfCSV.select(struct("InvoiceNo","Description").as("Complex"))
  dfComplex.createOrReplaceTempView("tempcomplexview")
  dfComplex.printSchema()
  dfComplex.select($"Complex".getField("InvoiceNo"),$"Complex".getField("Description"),
  $"Complex.*").show(3, truncate = false)

  //creating manual schema using struct and manual data
  val manualComplexSchema = new StructType().
    add("Name",new StructType()
      .add("FirstName",StringType)
      .add("LastName",StringType))
    .add("Class",StringType)
    .add("RollNo",IntegerType)

  val manualComplexData = Seq(
    Row(Row("Usha Devi","Patturaj"),"Seventh Std",37),
    Row(Row("Sai Prasad","Gorre"),"Fifth Std",15)
  )

  val rddSchool = spark.sparkContext.parallelize(manualComplexData)
  val dfSchool = spark.createDataFrame(rddSchool,manualComplexSchema)
  dfSchool.printSchema()
  val dfNormalSchool =dfSchool.withColumn("FirstName",$"Name".getField("FirstName"))
    .withColumn("LastName",$"Name".getField("LastName"))
    .withColumn("Class",$"Class")
    .withColumn("RollNumber",$"RollNo")
    .select("FirstName","LastName","Class","RollNumber")
  dfNormalSchool.show(truncate =false)

  //showing json format
  val json = manualComplexSchema.prettyJson
  println(json)
  import spark.implicits._
  //Arrays
  val dfArray = dfCSV.withColumn("SplittedDesc",split($"Description"," "))
  dfArray.show(5,truncate = false)
  dfArray.selectExpr("SplittedDesc[0]","SplittedDesc[1]").show(5,truncate = false)
  dfArray.select(size($"SplittedDesc")).show(5,truncate = false)
  dfArray.select(array_contains($"SplittedDesc","WHITE")).show(5,truncate = false)

  //explode
  dfArray.withColumn("exploded",explode($"SplittedDesc"))
    .select("InvoiceNo","SplittedDesc","exploded").show(5, truncate = false)
  //Map -key value pair
  dfCSV.select(map($"Description",$"InvoiceNo").alias("MapComplexType"))
    .selectExpr("MapComplexType['WHITE METAL LANTERN']").show(10,truncate = false)

  dfCSV.select(map($"Description",$"InvoiceNo").alias("MapComplexType"))
    .selectExpr("explode(MapComplexType)").show(10,truncate = false)

}
