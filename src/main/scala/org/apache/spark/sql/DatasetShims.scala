package org.apache.spark.sql

trait DatasetShims {
  implicit class DatasetHelper[T](ds: Dataset[T]) {
    def toShowString(numRows: Int = 20, truncate: Int = 20, vertical: Boolean = false): String =
      "\n" + ds.showString(numRows, truncate, vertical)
  }
}