package com.clarify.partitions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object PartitionDiagnostics {

  def getPartitionsAndCount(sql_ctx: SQLContext, view: String, result_view: String, sampling_fraction: Float): DataFrame = {
    val loaded_df: DataFrame = sql_ctx.table(view)
    loaded_df.show()
    val my_rdd: RDD[Row] = loaded_df.rdd.mapPartitionsWithIndex((index, iter) => _mapPartition(index, iter, sampling_fraction))
    val aStruct = new StructType(
      Array(
        StructField("partition_id", IntegerType, nullable = false),
        StructField("size", DoubleType, nullable = false),
        StructField("first", StringType, nullable = true)
      )
    )
    val df: DataFrame = sql_ctx.createDataFrame(my_rdd, aStruct)
    df.createOrReplaceTempView(result_view)
    // df.show(numRows = 1000, truncate = false)
    df
  }

  private def _mapPartition(index: Int, iterator: Iterator[Row], sampling_fraction: Float) = {
    if (iterator.isEmpty) Iterator(Row(index, 0d, ""))
    else if (index >= 0) {
      val first_item = iterator.next()(0).toString
      val countItems = iterator.size.toDouble + 1 // +1 to account for the first element we read
      Iterator(Row(index, countItems, first_item))
    }
    else Iterator(Row(index, -1d, "fall-through"))
  }
}
