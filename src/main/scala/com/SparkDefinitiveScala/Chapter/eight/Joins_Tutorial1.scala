package com.SparkDefinitiveScala.Chapter.eight
import com.SparkDefinitiveScala.Chapter.one.Context
import org.apache.spark.sql.functions._
object Joins_Tutorial1 extends App with Context{
  import spark.implicits._
  val person = Seq((0, "Bill Chambers", 0, Seq(100)),
                    (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
                    (2, "Michael Armbrust", 1, Seq(250, 100))).toDF("id", "name", "graduate_program", "spark_status")

  val graduateProgram = Seq((0, "Masters", "School of Information", "UC Berkeley"),
                            (2, "Masters", "EECS", "UC Berkeley"),
                            (1, "Ph.D.", "EECS", "UC Berkeley")).toDF("id", "degree", "department", "school")

  val sparkStatus = Seq((500, "Vice President"),
                        (250, "PMC Member"),
                        (100, "Contributor")).toDF("id", "status")

  person.createOrReplaceTempView("Person_View")
  graduateProgram.createOrReplaceTempView("Graduate_View")
  sparkStatus.createOrReplaceTempView("Spark_View")

  person.join(graduateProgram,person("id") === graduateProgram("id")).show(truncate = false)
  person.join(graduateProgram,person("id") === graduateProgram("id"),"inner").show(truncate = false)
  person.join(graduateProgram,Seq("id"),"inner").show(truncate = false)
  person.join(graduateProgram,Seq("id"),"left_outer").show(truncate = false)
  person.join(graduateProgram,Seq("id"),"left").show(truncate = false)
  person.join(graduateProgram,Seq("id"),"right_outer").show(truncate = false)
  person.join(graduateProgram,Seq("id"),"right").show(truncate = false)
  person.join(graduateProgram,Seq("id"),"left_anti").show(truncate = false)
  person.join(graduateProgram,Seq("id"),"left_semi").show(truncate = false)
  person.crossJoin(graduateProgram).show(truncate = false)

  //joining on complex types
  person.withColumnRenamed("id","personId").join(sparkStatus,expr("array_contains(spark_status,id)")).show(truncate = false)

  //handling duplicate columns in join
  val graduateProgram_renamed = graduateProgram.withColumnRenamed("id","graduate_program")
  val graduateProgram_renamed2 = graduateProgram.withColumnRenamed("id","graduate_program_id")
  person.join(graduateProgram_renamed,"graduate_program").select("graduate_program").show(truncate = false)
  person.join(graduateProgram_renamed,person("graduate_program") === graduateProgram_renamed("graduate_program"))
    .drop(person.col("graduate_program")).show(truncate = false)
  person.join(graduateProgram_renamed2,person("graduate_program") === graduateProgram_renamed2("graduate_program_id")).show(truncate = false)
}
