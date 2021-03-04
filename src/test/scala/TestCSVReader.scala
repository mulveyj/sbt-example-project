package com.newday.example

import org.apache.spark.sql.types.StructType
import org.scalatest._
import org.apache.spark.sql.{Encoders, SparkSession}

class TestCSVReader extends FunSuite with BeforeAndAfter {
  implicit var spark: SparkSession = _

  before {
    spark = SparkSession.builder
      .master("local")
      .appName("TestSession")
      .getOrCreate
  }

  test("CSVReader reads a simple file to a dataset") {
    val path = getClass.getClassLoader.getResource("testmoviedata.dat").getPath
    val resultDf = CSVReader.read[Movie](path)
    assert(resultDf.count === 5)
  }

  test("CSVReader reads another datatype") {
    val path = getClass.getClassLoader.getResource("testratingdata.dat").getPath
    val resultDf = CSVReader.read[Rating](path)
    assert(resultDf.count === 8)
  }

  after {
    spark.stop()
  }
}
