package com.newday.example

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

case class RatingReport(ratingMovieID: Int,
                        movieTitle: String,
                        movieGenre: String,
                        averageRating: Double,
                        maxRating: Int,
                        minRating: Int)
object Processor {

  def processMovieRatingAggregates(movieDf:Dataset[Movie], ratingDf:Dataset[Rating])(implicit spark: SparkSession):Dataset[RatingReport] = {
    import spark.implicits._
    val reducedRatingDf = ratingDf
                            .drop("ratingTimestamp")
                            .withColumnRenamed("movieId", "ratingMovieId")
    val movieDataWithRatings = movieDf
                              .join(reducedRatingDf,$"ratingMovieID"===$"movieId", "left")
                              .drop("movieId")
    movieDataWithRatings
        .select("ratingMovieID","movieTitle", "movieGenre", "starRating")
        .groupBy("ratingMovieID","movieTitle", "movieGenre")
        .agg(
          avg($"starRating").alias("averageRating"),
          max($"starRating").alias("maxRating"),
          min($"starRating").alias("minRating")
        ).as[RatingReport]
  }
}
