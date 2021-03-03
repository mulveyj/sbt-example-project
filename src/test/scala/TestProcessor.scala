package com.newday.example

import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.spark.sql.functions._

class TestProcessor extends FunSuite with BeforeAndAfter{
  implicit var spark: SparkSession = _
  val moviePath: String = getClass.getClassLoader.getResource("testmoviedata.dat").getPath
  val ratingPath: String = getClass.getClassLoader.getResource("testratingdata.dat").getPath
  val columnNamesMovie = Seq("movieID", "movieTitle", "movieGenre")
  val columnNamesRating = Seq("userId", "movieId", "starRating", "timeStamp")

  before {
    spark = SparkSession.builder
      .master("local")
      .appName("TestSession")
      .getOrCreate
  }

  test("processor returns aggregated dataset"){
    val movieDf = CSVReader.read[Movie](moviePath, columnNamesMovie)
    val ratingDf = CSVReader.read[Rating](ratingPath, columnNamesRating)
    val resultDf = Processor.processMovieRatingAggregates(movieDf, ratingDf)
    assert(resultDf.count == 5)
  }


  test("processor returns correct columns"){
    val movieDf = CSVReader.read[Movie](moviePath, columnNamesMovie)
    val ratingDf = CSVReader.read[Rating](ratingPath, columnNamesRating)
    val resultDf = Processor.processMovieRatingAggregates(movieDf, ratingDf)
    assert(List("ratingMovieID", "movieTitle", "movieGenre", "averageRating", "maxRating", "minRating")
      .toArray
      .sameElements(resultDf.columns)
    )
  }

  test("processor returns correct aggregation"){
    val movieDf = CSVReader.read[Movie](moviePath, columnNamesMovie)
    val ratingDf = CSVReader.read[Rating](ratingPath, columnNamesRating)
    val resultDf = Processor.processMovieRatingAggregates(movieDf, ratingDf)
    val movie = resultDf.where(col("ratingMovieId")===1).collect
    val expected = List(1, "Toy Story (1995)","Animation|Children's|Comedy", 3.5, 4, 3).toArray
    expected.zipWithIndex.foreach{case (x, i) => assert(x == movie(0).productElement(i))}
  }

  after {
    spark.close
  }
}
