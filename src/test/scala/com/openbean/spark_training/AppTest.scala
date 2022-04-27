package com.openbean.spark_training

import org.junit._
import Assert._
import org.apache.spark.sql.{Dataset, SparkSession}

class ReviewerTestReader(implicit sparkSession: SparkSession) extends Reader[Reviewer] {
    override def getData(): Dataset[Reviewer] = {
        import sparkSession.implicits._
        Seq(Reviewer(id = "dfs", name = "John", age = 45, gender = "Male", salary = 4.5)).toDS()
    }
}

@Test
class AppTest {

    @Test
    def testOK() = {
        implicit val spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
        val data = spark.read.json("data/reviewers_small.json.gz")
        val dataLength = Main.getNumColumns(data)
        assertEquals(dataLength,5)
    }
    @Test
    def test2() = {
        implicit val spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
        val data = spark.read.json("data/reviewers_small.json.gz")
        val dataLength = Main.getNumColumns(data)
        assertEquals(dataLength,5)
    }
    @Test
    def testReader() = {
        implicit val spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
        val data = spark.read.json("data/reviewers_small.json.gz")
        val dataLength = Main.getNumColumns(data)
        assertEquals(dataLength,5)
    }


//    @Test
//    def testKO() = assertTrue(false)

}


