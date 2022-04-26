package com.openbean.spark_training

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Main {


  def getNumColumns(data: DataFrame)(implicit spark: SparkSession) :Int = {
    data.columns.length
    //println("getting num of columns")
  }

  def getEpression(id: String) = {
    col("reviewerID") === id
  }


  def main(args: Array[String]): Unit = {
    println("Ahoj")
    val spark = SparkSession
      .builder()
      .appName("Test")
      .master("local[*]")
      .getOrCreate()

    print(spark.version)

    val reviewers = spark
      .read
      .json("data/reviewers_small.json.gz")

    //reviewers.createOrReplaceTempView("rews")

    //val ids = spark.sql("select lower(id) from rews")

    //reviewers.show(false)

    val reviews = spark.read.json("data/reviews_small.json.gz")
    import spark.implicits._
    val result = reviews
      .join(reviewers, col("reviewerID") === col("id"))
      .groupBy("reviewerID", "name", "gender", "age")
      .agg(
        count("*").alias("Number_of_reviews")
      )
      .sort(desc("Number_of_reviews"))
      .withColumnRenamed("Number_of_reviews", "num_rews")
      .withColumn("name", regexp_replace($"name", """["\.]""",""))
      .withColumn("names", split(col("name"), " ") )
      .withColumn("firstname", $"names".getItem(0))
      .withColumn("middle_name", when(size($"names") > 2, $"names".getItem(1)).otherwise(null))
      .withColumn("last_name", when(size($"names") > 2, $"names".getItem(2)).otherwise($"names".getItem(1)))
      .show(false)

    //spark.read.json("data/reviewers_small.json.gz").printSchema()

  }
}
