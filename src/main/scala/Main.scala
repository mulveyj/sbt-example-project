package com.newday.example
import org.apache.spark.sql.SparkSession

// no logging configured
object MainLocal extends App {
  val (pathToMovieFile, pathToRatingsFile, outputDirPathStem) = (args(0), args(1), args(2))
  val appName = "NewDayDemo"
  implicit val spark: SparkSession = SparkSession.builder
    .master("local")
    .appName(appName)
    .getOrCreate

  Runner.run(pathToMovieFile, pathToRatingsFile, outputDirPathStem)
}


object Main extends App {
  val (pathToMovieFile, pathToRatingsFile, outputDirPathStem) = (args(0), args(1), args(2))
//  config options should be passed as arguments to spark-submit
  implicit val spark: SparkSession = SparkSession.builder.getOrCreate

  Runner.run(pathToMovieFile, pathToRatingsFile, outputDirPathStem)
}


object Runner {
  def run(pathToMovieFile: String, pathToRatingsFile:String, outputDirPathStem: String)(implicit spark: SparkSession):Unit = {
    val movieDf = CSVReader.read[Movie](pathToMovieFile)
    val ratingsDf = CSVReader.read[Rating](pathToRatingsFile)
    val movieRatingsDf = Processor.processMovieRatings(movieDf, ratingsDf)
    ParquetWriter.write(movieDf, s"${outputDirPathStem}/movies", 3)
    ParquetWriter.write(ratingsDf, s"${outputDirPathStem}/ratings", 10)
    ParquetWriter.write(movieRatingsDf, s"${outputDirPathStem}/ratingsreport", 3)
    spark.close
  }
}
