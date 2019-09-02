package com.clarify.buckets

import java.util

import org.apache.spark.sql.functions.{col, hash, lit, pmod}
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.JavaConverters

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

  import java.util

  def getSeqString(list: util.ArrayList[String]): Seq[String] = JavaConverters.asScalaIteratorConverter(list.listIterator()).asScala.toSeq

  def readAsBucketWithPartitions(sql_ctx: SQLContext, view: String, numBuckets: Int, location: String, bucketColumns: util.ArrayList[String]): DataFrame = {

    require(bucketColumns.size() == 1 || bucketColumns.size() == 2, s"bucketColumns length, ${bucketColumns.size()} , is not supported")
    // get schema from parquet file without loading data from it
    val df = sql_ctx.read.format("parquet")
      .load(location)
    df.createOrReplaceTempView(s"${view}_temp")
    val df_schema = sql_ctx.sql(s"DESCRIBE ${view}_temp")
    val columns = for (x <- df_schema.collect()) yield x
    sql_ctx.sql(s"DROP VIEW IF EXISTS default.${view}_temp") // done with view
    // drop the raw table if it exists
    val raw_table_name = s"$view"
    sql_ctx.sql(s"DROP TABLE IF EXISTS default.$raw_table_name")
    val bucket_by_text = getSeqString(bucketColumns).mkString(",")
    // have to use CREATE TABLE syntax since that supports bucketing
    var text = s"CREATE TABLE IF NOT EXISTS $raw_table_name ("
    text += columns.map(column => s"\n${column(0)} ${column(1)}").mkString(",")
    text += ")\n"
    text +=
      s"""
            USING org.apache.spark.sql.parquet
            OPTIONS (
              path "$location"
            )
            CLUSTERED BY ($bucket_by_text) SORTED BY ($bucket_by_text) INTO $numBuckets BUCKETS
            """
    println(text)
    sql_ctx.sql(text)
    val result_df = sql_ctx.table(raw_table_name)
    result_df
  }
}
