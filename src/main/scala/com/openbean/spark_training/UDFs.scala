package com.openbean.spark_training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UDFs extends App{
  def concatenate(i : Employee): String = {
    s"${i.id}-${i.name}-${i.address.getOrElse("No Address Specified")}"
  }

  val spark = SparkSession
    .builder()
    .appName("Test")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  case class Employee(id: Long, name: String, address: Option[String])

  val df = Seq(Employee(1,"Petr", Some("Mirov, cela 3")),
    Employee(3,"Petr2", Some("Andel")),
    Employee(2,"Petr3", None))
    .toDF()
    .as[Employee]

  df.filter(_.id == 1 ).map(concatenate(_)).toDF("Concatenated").show()
  case class Reviewer(age: Long, gender: String, id: String, name: String, salary: Double)
  print(spark.version)
  import spark.implicits._
  val reviewers = spark
    .read
    .json("data/reviewers_small.json.gz")
    .as[Reviewer]
    .map(i =>
      Reviewer(i.age, i.gender, i.id, i.name.capitalize,i.salary))

  reviewers.show(false)



  val convertCase =  (strQuote:String) => {
    val arr = strQuote.split(" ")
    arr.map(f=>  f.substring(0,1).toUpperCase + f.substring(1,f.length)).mkString(" ")
  }

  //val convertUDF = udf((str: String) => str.capitalize)
  val convertUDF = udf(convertCase)
  reviewers.select($"name",
    convertUDF(col("name")).as("changed") ).show(false)

  spark.udf.register("convertUDF", convertCase)

  reviewers.createOrReplaceTempView("reviewers")
  spark.sql("select convertUDF(name) from reviewers").show()

}
