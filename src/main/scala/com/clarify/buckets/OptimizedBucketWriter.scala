package com.clarify.buckets

import java.util

import org.apache.spark.sql.functions.{col, hash, lit, pmod}
import org.apache.spark.sql.{DataFrame, SQLContext}

object OptimizedBucketWriter {

  def saveAsBucket(sql_ctx: SQLContext, view: String, numBuckets: Int, location: String, bucketColumn: String): DataFrame = {

    val df: DataFrame = sql_ctx.table(view)

    df
      .withColumn("bucket",
        pmod(
          hash(
            col(bucketColumn)),
          lit(numBuckets)
        )
      )
      .repartition(numBuckets, col("bucket"))
      .write
      .format("parquet")
      .bucketBy(numBuckets, bucketColumn)
      .sortBy(bucketColumn)
      .option("path", location)
      .saveAsTable("table_name")

    df
  }

  def quoteIdentifier(name: String): String = {
    // Escapes back-ticks within the identifier name with double-back-ticks, and then quote the
    // identifier with back-ticks.
    "`" + name.replace("`", "``") + "`"
  }

  def saveAsBucketWithPartitions(sql_ctx: SQLContext, view: String, numBuckets: Int, location: String, bucketColumns: util.ArrayList[String]): DataFrame = {

    require(bucketColumns.size() == 1 || bucketColumns.size() == 2, s"bucketColumns length, ${bucketColumns.size()} , is not supported")

    val df: DataFrame = sql_ctx.table(view)

    // this is a total hack for now
    if (bucketColumns.size() == 1) {
      df
        .withColumn("bucket",
          pmod(
            hash(
              col(bucketColumns.get(0))
            ),
            lit(numBuckets)
          )
        )
        .repartition(numBuckets, col("bucket"))
        .write
        .format("parquet")
        .partitionBy("bucket")
        .bucketBy(numBuckets, bucketColumns.get(0))
        .sortBy(bucketColumns.get(0))
        .option("path", location)
        .saveAsTable(s"temp_$view")
    }
    else if (bucketColumns.size() == 2) {
      df
        .withColumn("bucket",
          pmod(
            hash(
              col(bucketColumns.get(0)),
              col(bucketColumns.get(1))
            ),
            lit(numBuckets)
          )
        )
        .repartition(numBuckets, col("bucket"))
        .write
        .format("parquet")
        .partitionBy("bucket")
        .bucketBy(numBuckets, bucketColumns.get(0), bucketColumns.get(1))
        .sortBy(bucketColumns.get(0), bucketColumns.get(1))
        .option("path", location)
        .saveAsTable(s"temp_$view")
    }

    df
  }
}
