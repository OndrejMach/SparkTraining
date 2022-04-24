package com.openbean.spark_training

import org.junit._
import Assert._
import org.apache.spark.sql.SparkSession

@Test
class AppTest {

    @Test
    def testOK() = {
        implicit val spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
        val data = spark.read.json("data/reviewers_small.json.gz")
        val dataLength = Main.getNumColumns(data)
        assertEquals(dataLength,5)
    }

//    @Test
//    def testKO() = assertTrue(false)

}


