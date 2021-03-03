package com.newday.example

import org.apache.spark.sql.SparkSession

object Main extends App {
  val (pathToMovieFile, pathToRatingsFile, outputFilePathStem) = (args(0), args(1), args(2))
  val appName = "NewDayDemo"

  def run(pathToMovieFile: String, pathToRatingsFile:String, outputFileStem: String):Unit = {
    val spark = SparkSession.builder
      .master("local")
      .appName(appName)
      .getOrCreate
    println(pathToMovieFile, pathToRatingsFile, outputFilePathStem)
    // df1 = CSVReader.read(path1)
    // df2 = CSVReader.read(path2)
    // df3 = Processor.processAverages(df1, df2)
    //filename1 = etc
    // ParquetWriter.write(df1, filename1)
    // ParquetWriter.write(df2, filename2)
    // ParquetWriter.write(df3, filename3)
    spark.close
  }

  run(pathToMovieFile, pathToRatingsFile, outputFilePathStem)
}
