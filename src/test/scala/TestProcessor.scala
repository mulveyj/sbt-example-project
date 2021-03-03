package com.newday.example

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

class TestProcessor extends FunSuite with BeforeAndAfter{
  implicit var spark: SparkSession = _
  before {
    spark = SparkSession.builder
      .master("local")
      .appName("TestSession")
      .getOrCreate
  }

  test("processor returns aggregated dataset"){
    val moviePath = getClass.getClassLoader.getResource("testmoviedata.dat").getPath
    val ratingPath = getClass.getClassLoader.getResource("testratingdata.dat").getPath
    val columnNamesMovie = Seq("movieID", "movieTitle", "movieGenre")
    val columnNamesRating = Seq("userId", "movieId", "starRating", "timeStamp")
    val movieDf = CSVReader.read[Movie](moviePath, columnNamesMovie)
    val ratingDf = CSVReader.read[Rating](ratingPath, columnNamesRating)
    val resultDf = Processor.process(movieDf, ratingDf)
    assert(resultDf.count == 5)
  }

  after {
    spark.close
  }
}
