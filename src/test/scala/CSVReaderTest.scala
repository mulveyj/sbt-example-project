package com.newday.example

import org.scalatest._
import org.apache.spark.sql.SparkSession

class TestCSVReader extends FunSuite with BeforeAndAfter {
  implicit var spark: SparkSession = _

  before {
    spark = SparkSession.builder
      .master("local")
      .appName("TestSession")
      .getOrCreate
  }

  test("CSVReader reads a simple file to a dataset") {
    val path = getClass().getClassLoader.getResource("testmoviedata.dat").getPath
    val resultDf = CSVReader.read(path)
    assert(resultDf.count === 5)
  }

  after {
    spark.stop()
  }
}
