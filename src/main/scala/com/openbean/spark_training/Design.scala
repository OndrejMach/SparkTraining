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
  def processData(): Unit
}

class ProcessData(reader: Reader[Reviewer], writer: Writer[Reviewer]) extends Processor {
  override def processData(): Unit = {
    val input  = reader.getData()
    val result= input.filter("age = 45")
    writer.writeData(result)
  }
}

class ReviewersReader(path: String )(implicit sparkSession: SparkSession) extends Reader[Reviewer] {
  def getData() = {
    import sparkSession.implicits._
    sparkSession.read.json(path).as[Reviewer]
  }
}

class ReviewerTestReader(implicit sparkSession: SparkSession) extends Reader[Reviewer] {
  override def getData(): Dataset[Reviewer] = {
    import sparkSession.implicits._
    Seq(Reviewer(id = "dfs", name = "John", age = 45, gender = "Male",salary = 4.5)).toDS()
  }
}


object Design {
  def main(args: Array[String]): Unit = {
    implicit val sparkSession = SparkSession.builder().master("local[*]").appName("production").getOrCreate()
    val reader : Reader[Reviewer] = new ReviewerTestReader()
    val writer : Writer[Reviewer] = new OnScreenWriter()

    val processor = new ProcessData(reader,writer)

    processor.processData()


  }
}
