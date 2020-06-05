package com.SparkDefinitiveScala.Chapter.twelve

import com.SparkDefinitiveScala.Chapter.nine.Context
import scala.util.Random

object AdvancedRDD_Tutorial1 extends App with Context{
  val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
    .split(" ")
  val rdd_spark = spark.sparkContext.parallelize(myCollection,2)
  //key value pair creation using map
  val key_Val = rdd_spark.map(word => (word.toLowerCase,1))
  key_Val.foreach(println(_))

  //key value pair using keyBy
  val keyBy = rdd_spark.keyBy(word => word.toLowerCase.toSeq.head.toString)
  keyBy.foreach(print(_))

  //mapValues
  val mapValues = keyBy.mapValues(word => word.toUpperCase)
  mapValues.foreach(print(_))

  //flatmapvalues
  val flatMapValues = keyBy.flatMapValues(word => word.toUpperCase)
  flatMapValues.foreach(print(_))

  //keys,values,lookup
  val keys = keyBy.keys
  val values = keyBy.values
  val lookup = keyBy.lookup("S")
  keys.foreach(print(_))
  println()
  values.foreach(print(_))
  println()
  lookup.foreach(println(_))

  //SampleBykey
  val distinctChars = rdd_spark.flatMap(word => word.toLowerCase().toSeq).distinct().collect()
  val sampleMap = distinctChars.map(c => (c, new Random().nextDouble())).toMap
  val sampleByKey = rdd_spark.map(word => (word.toLowerCase.toSeq(0),word))
    .sampleByKey(withReplacement = true, sampleMap,seed = 6L).collect()

  sampleByKey.foreach(println(_))

  //aggregations
  val keyChars = rdd_spark.flatMap(word => word.toLowerCase().toSeq)
  val keyCharwithValues = keyChars.map(word => (word,1))
  println(keyCharwithValues.countByKey())


}
