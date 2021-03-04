package com.newday.example
import org.apache.spark.sql.SparkSession

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
  implicit val spark: SparkSession = SparkSession.builder.getOrCreate

  Runner.run(pathToMovieFile, pathToRatingsFile, outputDirPathStem)
}


object Runner {
  def run(pathToMovieFile: String, pathToRatingsFile:String, outputDirPathStem: String)(implicit spark: SparkSession):Unit = {
    val movieDf = CSVReader.read[Movie](pathToMovieFile, Seq("movieId", "movieTitle", "movieGenre"))
    val ratingsDf = CSVReader.read[Rating](pathToRatingsFile, Seq("userId", "movieId", "starRating", "timeStamp"))
    val aggregatedDf = Processor.processMovieRatingAggregates(movieDf, ratingsDf)
    ParquetWriter.write(movieDf, s"${outputDirPathStem}/movies", 3)
    ParquetWriter.write(ratingsDf, s"${outputDirPathStem}/ratings", 10)
    ParquetWriter.write(aggregatedDf, s"${outputDirPathStem}/ratingsreport", 3)
    spark.close
  }
}
