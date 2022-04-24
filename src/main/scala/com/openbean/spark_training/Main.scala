package com.openbean.spark_training

import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {
  def getNumColumns(data: DataFrame)(implicit spark: SparkSession) = {
    data.columns.length
  }


  def main(args: Array[String]): Unit = {
    println("Ahoj")
    implicit val spark = SparkSession
      .builder()
      .appName("Test")
      .master("local[*]")
      .getOrCreate()

    print(spark.version)

    spark.read.json("data/reviewers_small.json.gz").printSchema()

  }
}
