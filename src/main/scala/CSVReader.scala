package com.newday.example

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Encoder, Encoders}
import scala.reflect.runtime.universe.TypeTag


case class Movie(movieId: Long, movieTitle: String, movieGenre: String)
case class Rating(userId: Long, movieId: Long, starRating: Integer, timeStamp: Long)

object CSVReader {
  val delimiter = "::" // could be made configurable

  // Clearly it would be better to work with Path variables rather than strings
  def read[T<:Product: TypeTag](fileName: String)(implicit spark: SparkSession):Dataset[T] = {
    implicit val enc: Encoder[T] = Encoders.product[T]
    val dfRead: Dataset[Row] = spark.read.format("csv").option("header", "false").option("delimiter", delimiter).option("inferSchema", "true").load(fileName)
    val columnNames = Encoders.product[T].schema.toList.map(s => s.name)
    val columnNameMap = mapColumnNames(columnNames)
    val dfReadRenamedCols = columnNameMap.foldLeft(dfRead)((ds, cn) => ds.withColumnRenamed(cn._1, cn._2))
    dfReadRenamedCols.as[T]
  }

  private def mapColumnNames(columnNames: Seq[String]):Seq[Tuple2[String, String]] = {
    columnNames.zipWithIndex.map(e => (s"_c${e._2}", e._1))
  }

}
