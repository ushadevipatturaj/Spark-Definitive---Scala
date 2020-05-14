package com.SparkDefinitiveScala.Chapter.five
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructType
import com.SparkDefinitiveScala.Chapter.one.Context
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

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

  //another way of creating DF from manual row
  import spark.implicits._
  val dffromRDD1 = Seq(("Usha",122,true)).toDF("Name","Id","Day_Scholar")
  dffromRDD1.show()

  //select,selectexpr
  dfJson.select("DEST_COUNTRY_NAME").show(5)
  dfJson.select("DEST_COUNTRY_NAME","ORIGIN_COUNTRY_NAME").show(5)
  dfJson.select(dfJson.col("ORIGIN_COUNTRY_NAME"),
    col("ORIGIN_COUNTRY_NAME"),
    column("ORIGIN_COUNTRY_NAME"),
    'DEST_COUNTRY_NAME,
    $"DEST_COUNTRY_NAME",
    expr("DEST_COUNTRY_NAME")).show(5)
  dfJson.select(expr("DEST_COUNTRY_NAME as destination_country")).show(2)
  dfJson.select($"DEST_COUNTRY_NAME".alias("dest_country")).show(2)
  dfJson.selectExpr("DEST_COUNTRY_NAME as destination_country","ORIGIN_COUNTRY_NAME").show(2)
  dfJson.selectExpr("*"," (ORIGIN_COUNTRY_NAME = DEST_COUNTRY_NAME) as within_country").show(3)
  dfJson.selectExpr("avg(count)","count(distinct(DEST_COUNTRY_NAME)) as total_destination_countries").show(5)

  //converting to spark types
  //literal
  dfJson.select(expr("*"),lit(1).as("One")).show(5)

  //adding a column to dataframe
  dfJson.withColumn("One",lit(1)).show(10)
  dfJson.withColumn("WithinCountry",expr("ORIGIN_COUNTRY_NAME = DEST_COUNTRY_NAME")).show(5)

  //renaming a column
  dfJson.withColumnRenamed("ORIGIN_COUNTRY_NAME","Origin_Country").show(3)

  //column names with spaces and -
  dfJson.withColumn("orgin country-1",$"ORIGIN_COUNTRY_NAME").show(2)
  dfJson.selectExpr("ORIGIN_COUNTRY_NAME as `This is new-column`").show(2)
  dfJson.drop("ORIGIN_COUNTRY_NAME").show(2)
  dfJson.withColumn("Long_Count",$"count".cast(LongType)).show(3)

  //filtering rows using filter and where
  dfJson.filter($"count" <2 ).show()
  dfJson.where($"count"<2 and  $"ORIGIN_COUNTRY_NAME"=!="Croatia").show()

  //getting distinct values from a dataframe
  dfJson.select($"ORIGIN_COUNTRY_NAME").distinct().show(5)
  dfJson.select("ORIGIN_COUNTRY_NAME","DEST_COUNTRY_NAME").distinct().show(5)
  val countofDistinct = dfJson.select("ORIGIN_COUNTRY_NAME").distinct().count()
  println(s"count of distinct ORIGIN_COUNTRY_NAME $countofDistinct")
}
