package com.newday.example

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Encoder, Encoders}


//case class Movie(movieID: Long, movieTitle: String, movieGenre: String)
//case class Rating(userId: Long, ratingId: Long, movieRating: Integer, timeStamp: Long)

object CSVReader {
  val delimiter = "::"

  def read(fileName: String)(implicit spark: SparkSession):Dataset[Row] = {
    //    implicit val enc: Encoder[Movie] = Encoders.kryo[Movie]
    val dfRead = spark.read.format("csv").option("header", "false").option("delimiter", delimiter).option("inferSchema", "true").load(fileName)
    val dfReadRenamedCols = dfRead.withColumnRenamed("_c0", "movieId").withColumnRenamed("_c1", "movieTitle").withColumnRenamed("_c2", "movieGenre")
    dfReadRenamedCols
  }

}
