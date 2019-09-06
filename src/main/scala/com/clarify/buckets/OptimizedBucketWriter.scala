package com.clarify.buckets

import java.util

import com.clarify.memory.MemoryDiagnostics
import org.apache.log4j.Logger
import org.apache.spark.SparkException
import org.apache.spark.sql.functions.{col, hash, lit, pmod}
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.JavaConverters

object OptimizedBucketWriter {

  def saveAsBucketWithPartitions(sql_ctx: SQLContext, view: String, numBuckets: Int,
                                 location: String, bucketColumns: util.ArrayList[String]): Boolean = {
    println(s"[Scala] saveAsBucketWithPartitions: free memory before (MB): ${MemoryDiagnostics.get_free_memory()}")

    try {
      require(bucketColumns.size() == 1 || bucketColumns.size() == 2,
        s"[Scala] bucketColumns length, ${bucketColumns.size()} , is not supported.  We only support 1 and 2 right now.")

      val logger = Logger.getLogger(getClass.getName)
      println(s"[Scala] saveAsBucketWithPartitions: view=$view numBuckets=$numBuckets location=$location bucket_columns(${bucketColumns.size()})=$bucketColumns")
      val df: DataFrame = sql_ctx.table(view)

      // this is a total hack for now
      val table_name = s"temp_$view"
      sql_ctx.sql(s"DROP TABLE IF EXISTS default.$table_name")

      if (bucketColumns.size() == 1) {
        var my_df = df
        if (!df.columns.contains("bucket")) {
          println(s"[Scala] Adding bucket column to $view")
          my_df = df
            .withColumn("bucket",
              pmod(
                hash(
                  col(bucketColumns.get(0))
                ),
                lit(numBuckets)
              )
            )
            .repartition(numBuckets, col("bucket"))
        }


        //        val unique_buckets = my_df.select(col("bucket")).distinct().count()
        //        println(s"saveAsBucketWithPartitions: count: ${my_df.count()}")
        //        println(s"saveAsBucketWithPartitions: Number of buckets: $unique_buckets")
        //        println(s"[Scala] Caching df for $view")
        //        my_df = my_df.cache()
        //        println(s"[Scala] Finished caching df for $view")
        my_df
          .write
          .format("parquet")
          .partitionBy("bucket")
          .bucketBy(numBuckets, bucketColumns.get(0))
          .sortBy(bucketColumns.get(0))
          .option("path", location)
          .saveAsTable(table_name)

        //        my_df.unpersist(true)
        //        println(s"[Scala] REFRESH TABLE default.$table_name")
        //        sql_ctx.sql(s"REFRESH TABLE default.$table_name")
        println(s"[Scala] DROP TABLE default.$table_name")
        sql_ctx.sql(s"DROP TABLE default.$table_name")
      }
      else if (bucketColumns.size() == 2) {
        var my_df = df
        if (!df.columns.contains("bucket")) {
          println(s"[Scala] Adding bucket column to $view")
          my_df = df
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
        }

        // my_df.select("bucket", bucketColumns.get(0), bucketColumns.get(1)).show(numRows = 1000)

        //        val unique_buckets = my_df.select(col("bucket")).distinct().count()
        //        println(s"saveAsBucketWithPartitions: Number of buckets: $unique_buckets")
        //        println(s"[Scala] Caching df for $view")
        //        my_df = my_df.cache()
        //        println(s"[Scala] Finished caching df for $view")
        my_df
          .write
          .format("parquet")
          .partitionBy("bucket")
          .bucketBy(numBuckets, bucketColumns.get(0), bucketColumns.get(1))
          .sortBy(bucketColumns.get(0), bucketColumns.get(1))
          .option("path", location)
          .saveAsTable(table_name)

        //        my_df.unpersist(true)
        //        println(s"[Scala] REFRESH TABLE default.$table_name")
        //        sql_ctx.sql(s"REFRESH TABLE default.$table_name")
        println(s"[Scala] DROP TABLE default.$table_name")
        sql_ctx.sql(s"DROP TABLE default.$table_name")
      }

      println(s"[Scala] saveAsBucketWithPartitions: free memory after (MB): ${MemoryDiagnostics.get_free_memory()}")

      true
    }
    catch {
      case e: SparkException =>
        val cause = e.getCause
        println(s"[Scala] readAsBucketWithPartitions: Got SparkException: $cause")
        throw cause
      case unknown: Throwable =>
        println(s"[Scala] saveAsBucketWithPartitions: Got some other kind of exception: $unknown")
        throw unknown
    }
  }

  import java.util

  def getSeqString(list: util.ArrayList[String]): Seq[String] = JavaConverters.asScalaIteratorConverter(list.listIterator()).asScala.toSeq

  def readAsBucketWithPartitions(sql_ctx: SQLContext, view: String, numBuckets: Int, location: String, bucketColumns: util.ArrayList[String]): Boolean = {

    println(s"[Scala] readAsBucketWithPartitions: free memory before (MB): ${MemoryDiagnostics.get_free_memory()}")

    require(bucketColumns.size() == 1 || bucketColumns.size() == 2, s"bucketColumns length, ${bucketColumns.size()} , is not supported")
    println(s"[Scala] readAsBucketWithPartitions: view=$view numBuckets=$numBuckets location=$location bucket_columns(${bucketColumns.size()})=$bucketColumns")
    val logger = Logger.getLogger(getClass.getName)
    // get schema from parquet file without loading data from it
    val df = sql_ctx.read.format("parquet")
      .load(location)
    val temp_view = s"${view}_temp_bucket_reader"
    df.createOrReplaceTempView(temp_view)
    val columns = _getColumnSchema(sql_ctx, temp_view)
    sql_ctx.sql(s"DROP VIEW $temp_view") // done with view
    try {
      // sql_ctx.sql(s"DROP VIEW IF EXISTS default.$temp_view") // done with view
      // drop the raw table if it exists
      val raw_table_name = s"${view}_raw_buckets"
      sql_ctx.sql(s"DROP TABLE IF EXISTS default.$raw_table_name")
      //sql_ctx.sql(s"REFRESH TABLE default.$raw_table_name")
      val bucket_by_text = getSeqString(bucketColumns).mkString(",")
      // have to use CREATE TABLE syntax since that supports bucketing
      var text = s"CREATE TABLE $raw_table_name ("
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
      println(s"[Scala] REFRESH TABLE default.$raw_table_name")
      sql_ctx.sql(s"REFRESH TABLE default.$raw_table_name")
      // sql_ctx.sql(s"DESCRIBE EXTENDED $raw_table_name").show(numRows = 1000)
      val result_df = sql_ctx.table(raw_table_name)
      result_df.createOrReplaceTempView(view)
      // sql_ctx.sql(s"SELECT * FROM $view").explain(extended = true)
      println(s"[Scala] readAsBucketWithPartitions: free memory after (MB): ${MemoryDiagnostics.get_free_memory()}")

      true
    }
    catch {
      case e: SparkException =>
        val cause = e.getCause
        println(s"[Scala] readAsBucketWithPartitions: Got SparkException: $cause")
        throw cause
      case unknown: Throwable =>
        println(s"[Scala] readAsBucketWithPartitions: Got some other kind of exception: $unknown")
        throw unknown
    }
  }

  private def _getColumnSchema(sql_ctx: SQLContext, temp_view: String) = {
    val df_schema = sql_ctx.sql(s"DESCRIBE $temp_view")
    _getColumnSchemaFromDataFrame(df_schema)
  }

  private def _getColumnSchemaFromDataFrame(df_schema: DataFrame) = {
    val columns = df_schema.select(col("col_name"), col("data_type"))
      .rdd.map(x => x.toSeq.toArray).collect()
    columns
  }

  def checkpointBucketWithPartitions(sql_ctx: SQLContext, view: String, numBuckets: Int,
                                     location: String, bucketColumns: util.ArrayList[String]): Boolean = {

    if (!sql_ctx.table(view).isEmpty) {
      saveAsBucketWithPartitions(sql_ctx = sql_ctx, view = view, numBuckets = numBuckets, location = location, bucketColumns = bucketColumns)
      readAsBucketWithPartitions(sql_ctx = sql_ctx, view = view, numBuckets = numBuckets, location = location, bucketColumns = bucketColumns)
    }
    else {
      println(s"[Scala] $view was empty so did not bucket it")
      false
    }
  }

  def checkpointBucketWithPartitionsInMemory(sql_ctx: SQLContext, view: String, numBuckets: Int, location: String, bucketColumns: util.ArrayList[String]): Boolean = {
    val df = sql_ctx.table(view)
    val rdd = df.rdd
    rdd.cache()
    sql_ctx.createDataFrame(rdd, df.schema).createOrReplaceTempView(view)
    true
  }

}
