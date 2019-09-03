package com.clarify.partitions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object PartitionDiagnostics {

  def getPartitionsAndCount(sql_ctx: SQLContext, view: String, result_view: String, max_samples: Int): DataFrame = {
    val loaded_df: DataFrame = sql_ctx.table(view)
    val num_partitions = loaded_df.rdd.getNumPartitions
    val sampling_fraction: Float = Math.min(max_samples.toFloat / num_partitions, 1f)
    val sampling_mod: Int = (1 / sampling_fraction).toInt
    val my_rdd: RDD[Row] = loaded_df.rdd.mapPartitionsWithIndex((index, iterator) => _mapPartition(index, iterator, sampling_mod))
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

  private def _mapPartition(index: Int, iterator: Iterator[Row], sampling_mod: Int) = {
    if (iterator.isEmpty) Iterator(Row(index, 0d, "empty"))
    else if ((index % sampling_mod) == 0) {
      val first_item = iterator.next()(0).toString
      val countItems = iterator.size.toDouble + 1 // +1 to account for the first element we read
      Iterator(Row(index, countItems, first_item))
    }
    else Iterator(Row(index, -1d, "not sampled"))
  }
}
