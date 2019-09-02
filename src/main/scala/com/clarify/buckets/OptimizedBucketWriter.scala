package com.clarify.buckets

import org.apache.spark.sql.functions.{col, hash, lit, pmod}
import org.apache.spark.sql.{DataFrame, SQLContext}

class OptimizedBucketWriter {

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

  def saveAsBucketWithPartitions(sql_ctx: SQLContext, view: String, numBuckets: Int, location: String, bucketColumns: Array[String]): DataFrame = {

    require(bucketColumns.length == 1 || bucketColumns.length == 2, s"bucketColumns length, ${bucketColumns.length} , is not supported")

    val df: DataFrame = sql_ctx.table(view)

    // this is a total hack for now
    if (bucketColumns.length == 1) {
      df
        .withColumn("bucket",
          pmod(
            hash(
              col(bucketColumns(0))
            ),
            lit(numBuckets)
          )
        )
        .repartition(numBuckets, col("bucket"))
        .write
        .format("parquet")
        .partitionBy("bucket")
        .bucketBy(numBuckets, bucketColumns(0))
        .sortBy(bucketColumns(0))
        .option("path", location)
        .saveAsTable(s"temp_$view")
    }
    else if (bucketColumns.length == 2) {
      df
        .withColumn("bucket",
          pmod(
            hash(
              col(bucketColumns(0)),
              col(bucketColumns(1))
            ),
            lit(numBuckets)
          )
        )
        .repartition(numBuckets, col("bucket"))
        .write
        .format("parquet")
        .partitionBy("bucket")
        .bucketBy(numBuckets, bucketColumns(0), bucketColumns(1))
        .sortBy(bucketColumns(0), bucketColumns(1))
        .option("path", location)
        .saveAsTable(s"temp_$view")
    }

    df
  }
}
