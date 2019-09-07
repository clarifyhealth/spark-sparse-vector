package com.clarify.buckets

import java.util

import com.clarify.Helpers
import com.clarify.memory.MemoryDiagnostics
import org.apache.spark.SparkException
import org.apache.spark.sql.functions.{col, hash, lit, pmod}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Random

object OptimizedBucketWriter {

  val _LOGGER: Logger = LoggerFactory.getLogger(this.getClass.getName)

  def saveAsBucketWithPartitions(sql_ctx: SQLContext, view: String, numBuckets: Int,
                                 location: String, bucketColumns: util.ArrayList[String]): Boolean = {
    Helpers.log(s"saveAsBucketWithPartitions: free memory before (MB): ${MemoryDiagnostics.getFreeMemoryMB}")

    try {
      require(bucketColumns.size() == 1 || bucketColumns.size() == 2,
        s"bucketColumns length, ${bucketColumns.size()} , is not supported.  We only support 1 and 2 right now.")

      Helpers.log(s"saveAsBucketWithPartitions: view=$view numBuckets=$numBuckets location=$location bucket_columns(${bucketColumns.size()})=$bucketColumns")
      val df: DataFrame = sql_ctx.table(view)

      // val original_table_name = s"temp_$view"
      val rand = Random.alphanumeric.take(5).mkString("")
      val new_table_name = s"temp_${view}_____$rand"
      // val tableNames: Array[String] = sql_ctx.tableNames()

      if (bucketColumns.size() == 1) {
        var my_df = df
        if (!df.columns.contains("bucket")) {
          Helpers.log(s"Adding bucket column to $view")
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
        else {
          Helpers.log(s"Skipping adding bucket column since it exists $view")
        }

        my_df
          .write
          //.mode("overwrite")
          .format("parquet")
          //.partitionBy("bucket")
          .bucketBy(numBuckets, bucketColumns.get(0))
          .sortBy(bucketColumns.get(0))
          //.option("path", location)
          .saveAsTable(new_table_name)
        //        val unique_buckets = my_df.select(col("bucket")).distinct().count()
        //        Helpers._log(s"saveAsBucketWithPartitions: count: ${my_df.count()}")
        //        Helpers._log(s"saveAsBucketWithPartitions: Number of buckets: $unique_buckets")
        //        Helpers._log(s"Caching df for $view")
        //        my_df = my_df.cache()
        //        Helpers._log(s"Finished caching df for $view")


        //        my_df.unpersist(true)
        //        Helpers._log(s"REFRESH TABLE default.$original_table_name")
        //        sql_ctx.sql(s"REFRESH TABLE default.$original_table_name")
        //        Helpers._log(s"DROP TABLE default.$original_table_name")
        //        sql_ctx.sql(s"DROP TABLE default.$original_table_name")
      }
      else if (bucketColumns.size() == 2) {
        var my_df = df
        if (!df.columns.contains("bucket")) {
          Helpers.log(s"Adding bucket column to $view")
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
        } else {
          Helpers.log(s"Skipping adding bucket column since it exists $view")
        }

        // my_df.select("bucket", bucketColumns.get(0), bucketColumns.get(1)).show(numRows = 1000)

        //        val unique_buckets = my_df.select(col("bucket")).distinct().count()
        //        Helpers._log(s"saveAsBucketWithPartitions: Number of buckets: $unique_buckets")
        //        Helpers._log(s"Caching df for $view")
        //        my_df = my_df.cache()
        //        Helpers._log(s"Finished caching df for $view")
        my_df
          .write
          //.mode("overwrite")
          .format("parquet")
          //.partitionBy("bucket")
          .bucketBy(numBuckets, bucketColumns.get(0), bucketColumns.get(1))
          .sortBy(bucketColumns.get(0), bucketColumns.get(1))
          //          .option("path", location)
          .saveAsTable(new_table_name)

        //        my_df.unpersist(true)
        //        Helpers._log(s"REFRESH TABLE default.$original_table_name")
        //        sql_ctx.sql(s"REFRESH TABLE default.$original_table_name")
        //        Helpers._log(s"DROP TABLE default.$original_table_name")
        //        sql_ctx.sql(s"DROP TABLE default.$original_table_name")
        // sql_ctx.sql(s"DROP TABLE IF EXISTS default.$original_table_name")
      }

      sql_ctx.sql(s"REFRESH TABLE $new_table_name")
      sql_ctx.sql(s"DESCRIBE EXTENDED $new_table_name").show(numRows = 1000)

      Helpers.log(s"saveAsBucketWithPartitions: free memory after (MB): ${MemoryDiagnostics.getFreeMemoryMB}")
      val result_df = sql_ctx.table(new_table_name)
      result_df.createOrReplaceTempView(view)

      //      for (tableName <- tableNames.filter(t => t.startsWith(original_table_name))){
      //        Helpers._log(s"DROP TABLE default.$tableName")
      //        sql_ctx.sql(s"DROP TABLE default.$tableName")
      //      }

      true
    }
    catch {
      case e: SparkException =>
        val cause = e.getCause
        Helpers.log(s"readAsBucketWithPartitions: Got SparkException: $cause")
        throw cause
      case unknown: Throwable =>
        Helpers.log(s"saveAsBucketWithPartitions: Got some other kind of exception: $unknown")
        throw unknown
    }
  }


  def readAsBucketWithPartitions(sql_ctx: SQLContext, view: String, numBuckets: Int, location: String, bucketColumns: util.ArrayList[String]): Boolean = {

    Helpers.log(s"readAsBucketWithPartitions: free memory before (MB): ${MemoryDiagnostics.getFreeMemoryMB}")

    require(bucketColumns.size() == 1 || bucketColumns.size() == 2, s"bucketColumns length, ${bucketColumns.size()} , is not supported")
    Helpers.log(s"readAsBucketWithPartitions: view=$view numBuckets=$numBuckets location=$location bucket_columns(${bucketColumns.size()})=$bucketColumns")
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
      val bucket_by_text = Helpers.getSeqString(bucketColumns).mkString(",")
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
      Helpers.log(text)
      sql_ctx.sql(text)
      Helpers.log(s"REFRESH TABLE default.$raw_table_name")
      sql_ctx.sql(s"REFRESH TABLE default.$raw_table_name")
      // sql_ctx.sql(s"DESCRIBE EXTENDED $raw_table_name").show(numRows = 1000)
      val result_df = sql_ctx.table(raw_table_name)
      result_df.createOrReplaceTempView(view)
      // sql_ctx.sql(s"SELECT * FROM $view").explain(extended = true)
      Helpers.log(s"readAsBucketWithPartitions: free memory after (MB): ${MemoryDiagnostics.getFreeMemoryMB}")

      true
    }
    catch {
      case e: SparkException =>
        val cause = e.getCause
        Helpers.log(s"readAsBucketWithPartitions: Got SparkException: $cause")
        throw cause
      case unknown: Throwable =>
        Helpers.log(s"readAsBucketWithPartitions: Got some other kind of exception: $unknown")
        throw unknown
    }
  }

  def readAsBucketWithPartitions2(sql_ctx: SQLContext, view: String, numBuckets: Int, location: String, bucketColumns: util.ArrayList[String]): Boolean = {

    Helpers.log(s"readAsBucketWithPartitions: free memory before (MB): ${MemoryDiagnostics.getFreeMemoryMB}")

    require(bucketColumns.size() == 1 || bucketColumns.size() == 2, s"bucketColumns length, ${bucketColumns.size()} , is not supported")
    Helpers.log(s"readAsBucketWithPartitions: view=$view numBuckets=$numBuckets location=$location bucket_columns(${bucketColumns.size()})=$bucketColumns")
    val raw_table_name = s"temp_$view"
    Helpers.log(s"REFRESH TABLE default.$raw_table_name")
    try {
      sql_ctx.sql(s"REFRESH TABLE default.$raw_table_name")
      // sql_ctx.sql(s"DESCRIBE EXTENDED $raw_table_name").show(numRows = 1000)
      val result_df = sql_ctx.table(raw_table_name)
      result_df.createOrReplaceTempView(view)
      // sql_ctx.sql(s"SELECT * FROM $view").explain(extended = true)
      Helpers.log(s"readAsBucketWithPartitions: free memory after (MB): ${MemoryDiagnostics.getFreeMemoryMB}")

      true
    }
    catch {
      case e: SparkException =>
        val cause = e.getCause
        Helpers.log(s"readAsBucketWithPartitions: Got SparkException: $cause")
        throw cause
      case unknown: Throwable =>
        Helpers.log(s"readAsBucketWithPartitions: Got some other kind of exception: $unknown")
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

    Helpers.log(s"checkpointBucketWithPartitions for $view")
    if (!sql_ctx.table(view).isEmpty) {
      saveAsBucketWithPartitions(sql_ctx = sql_ctx, view = view, numBuckets = numBuckets, location = location, bucketColumns = bucketColumns)
      // readAsBucketWithPartitions2(sql_ctx = sql_ctx, view = view, numBuckets = numBuckets, location = location, bucketColumns = bucketColumns)
    }
    else {
      Helpers.log(s"$view was empty so did not bucket it")
      false
    }
  }

  def checkpointWithoutBuckets(sql_ctx: SQLContext, view: String, numBuckets: Int,
                               location: String, bucketColumns: util.ArrayList[String]): Boolean = {

    Helpers.log(s"checkpointWithoutBuckets for $view")
    if (!sql_ctx.table(view).isEmpty) {
      val df = sql_ctx.table(view)
      df.write.parquet(location)
      val result_df = sql_ctx.read.parquet(location)
      result_df.createOrReplaceTempView(view)
      sql_ctx.sql(s"REFRESH TABLE $view")
      sql_ctx.sql(s"DESCRIBE EXTENDED $view").show(numRows = 1000)
      true
    }
    else {
      Helpers.log(s"$view was empty so did not bucket it")
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
