package com.openbean.spark_training

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

case class Reviewer(age: Long, gender: String, id: String, name: String, salary: Double)

trait Reader[T] {
  def getData() : Dataset[T]
}

trait Writer[T] {
  def writeData(data: Dataset[T]) : Unit
}

class OnScreenWriter extends Writer[Reviewer]{
  override def writeData(data: Dataset[Reviewer]): Unit = data.show(false)
}

class GenericWriter extends Writer[Row]{
  override def writeData(data: DataFrame): Unit = data.show(false)
}

trait Processor {
  def processData(): Dataset[Reviewer]
}

class ProcessData(reader: Reader[Reviewer]) extends Processor {
  override def processData(): Dataset[Reviewer] = {
    val input  = reader.getData()
    input.filter("age = 45")
  }
}

class ReviewersReader(path: String )(implicit sparkSession: SparkSession) extends Reader[Reviewer] {
  def getData() = {
    import sparkSession.implicits._
    sparkSession.read.json(path).as[Reviewer]
  }
}

object Design {
  def main(args: Array[String]): Unit = {
    implicit val sparkSession = SparkSession.builder().master("local[*]").appName("production").getOrCreate()
    val reader : Reader[Reviewer] = new ReviewersReader()
    val writer : Writer[Reviewer] = new OnScreenWriter()

    val processor = new ProcessData(reader,writer)

    processor.processData()


  }
}
