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

  def saveAsBucketWithPartitioning(sql_ctx: SQLContext, view: String, numBuckets: Int, location: String, bucketColumn: String): DataFrame = {

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
      .partitionBy("bucket")
      .bucketBy(numBuckets, bucketColumn)
      .sortBy(bucketColumn)
      .option("path", location)
      .saveAsTable("table_name")

    df
  }

}
