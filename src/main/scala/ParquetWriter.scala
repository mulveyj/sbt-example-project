package com.newday.example

import org.apache.spark.sql.Dataset

object ParquetWriter {
  def write[T](df: Dataset[T], dirname: String, partitions: Int): Unit = {
    df.repartition(partitions).write.parquet(dirname)
  }

}
