package com.SparkDefinitiveScala.Chapter.nine

object DataSources_Tutorial1 extends App with Context {
  //reading csv file
  val csvFile = spark.read.format("csv")
    .option("inferSchema",value = true).option("mode","FAILFAST")
    .option("header",value = true)
    .load("D:\\Study_Materials\\spark-definitive-scala\\src\\main\\resources\\2015-summary.csv")

  //writing a dataframe to a file
  csvFile.write.mode("OVERWRITE")
    .option("sep",",").save("D:\\Study_Materials\\spark-definitive-scala\\src\\main\\resources\\2015-summary-fromcode.csv")

  val jsonFile = spark.read.option("inferSchema",value = true)
    .option("mode","FAILFAST").option("header",value = true)
    .format("json").load("D:\\Study_Materials\\spark-definitive-scala\\src\\main\\resources\\2015-summary.json")

  jsonFile.write.mode("OVERWRITE").format("json")
    .save("D:\\Study_Materials\\spark-definitive-scala\\src\\main\\resources\\2015-summary-fromcode.json")


}
