package com.clarify.buckets.v4

import java.util

import com.clarify.Helpers
import com.clarify.hdfs.HdfsHelper
import com.clarify.memory.MemoryDiagnostics
import com.clarify.retry.Retry
import org.apache.spark.SparkException
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.sys.process._

object OptimizedBucketWriter {

  val _LOGGER: Logger = LoggerFactory.getLogger(this.getClass.getName)

  def saveAsBucketWithPartitions(sql_ctx: SQLContext, view: String, numBuckets: Int,
                                 location: String,
                                 bucketColumns: util.ArrayList[String],
                                 sortColumns: util.ArrayList[String],
                                 name: String): Boolean = {
    Helpers.log(s"saveAsBucketWithPartitions v2: free memory before (MB): ${MemoryDiagnostics.getFreeMemoryMB}")
    require(location != null, "location cannot be null")
    require(numBuckets > 0, f"numBuckets $numBuckets should be greater than 0")
    require(bucketColumns.size() > 0, f"There were no bucket columns specified")
    require(sortColumns.size() > 0, f"There were no sort columns specified")

    // if folder exists then skip writing
    if (name != null && HdfsHelper.__folderWithDataExists(sql_ctx, location, name)) {
      Helpers.log(f"Folder $location already exists with data so skipping saving table")
      return true
    }

    val result = Retry.retry(5) {
      _saveBucketsToFileInternal(sql_ctx = sql_ctx, view = view, numBuckets = numBuckets,
        location = location,
        bucketColumns = bucketColumns,
        sortColumns = sortColumns,
        name = name,
        saveLocalAndCopyToS3 = false)
    }
    Await.result(result, 3 hours)
  }

  private def _saveBucketsToFileInternal(sql_ctx: SQLContext, view: String, numBuckets: Int,
                                         location: String,
                                         bucketColumns: util.ArrayList[String],
                                         sortColumns: util.ArrayList[String],
                                         name: String,
                                         saveLocalAndCopyToS3: Boolean): Boolean = {

    require(bucketColumns.size() > 0, f"There were no bucket columns specified")
    require(sortColumns.size() > 0, f"There were no sort columns specified")
    // To avoid S3 slowdown due to writing too many files, write to local and then copy to s3
    val localLocation = if (saveLocalAndCopyToS3 && location.startsWith("s3:")) f"/tmp/checkpoint/$name" else location
    try {
      if (name != null) {
        sql_ctx.sparkContext.setJobDescription(name)
      }
      Helpers.log(s"_saveBucketsToFileInternal v3: view=$view numBuckets=$numBuckets location=$location"
        + f" bucket_columns(${bucketColumns.size()})=$bucketColumns, sort_columns=$sortColumns")
      val df: DataFrame = sql_ctx.table(view)

      val table_name = s"temp_$view"
      sql_ctx.sql(s"DROP TABLE IF EXISTS default.$table_name")
      val my_df: DataFrame = addBucketColumnToDataFrame(df = df, view = view,
        numBuckets = numBuckets, bucketColumns = bucketColumns,
        sortColumns = sortColumns)

      val bucketColumnsSeq: Seq[String] = Helpers.getSeqString(bucketColumns).drop(1)
      val sortColumnsSeq: Seq[String] = Helpers.getSeqString(sortColumns).drop(1)

      Helpers.log(f"Saving to bucketed table $view")

      my_df
        .write
        .format("parquet")
        //.partitionBy("bucket")
        .bucketBy(numBuckets, colName = bucketColumns.get(0), colNames = bucketColumnsSeq: _*)
        .sortBy(colName = sortColumns.get(0), colNames = sortColumnsSeq: _*)
        .option("path", localLocation)
        .saveAsTable(table_name)

      Helpers.log(f"Finished saving to bucketed table $view")

      //      val df_after_save: DataFrame = sql_ctx.table(table_name)
      //      if (df_after_save.isEmpty) {
      //        Helpers.log(f"data frame was empty so writing via df.write view=$view location=$location")
      //        // if data frame is empty then write an empty data frame otherwise we get no file
      //        df_after_save.write.mode("overwrite").parquet(localLocation)
      //      }
      Helpers.log(s"DROP TABLE default.$table_name")
      sql_ctx.sql(s"DROP TABLE default.$table_name")

      if (saveLocalAndCopyToS3 && location.startsWith("s3:")) {
        Helpers.log(f"s3-dist-cp --s3Endpoint=s3.us-west-2.amazonaws.com --src=hdfs://$localLocation --dest=$location")
        val results = Seq("s3-dist-cp",
          "--s3Endpoint=s3.us-west-2.amazonaws.com",
          f"--src=hdfs://$localLocation",
          f"--dest=$location").!!.trim
        Helpers.log(results)
      }
      Helpers.log(s"_saveBucketsToFileInternal: free memory after (MB): ${MemoryDiagnostics.getFreeMemoryMB}")
      if (!location.startsWith("s3:")) {
        // print free space left
        HdfsHelper._printFreeSpace(sql_ctx.sparkContext)
      }
      true
    }
    catch {
      case e: SparkException =>
        val cause = e.getCause
        if (cause != null) {
          Helpers.log(s"readAsBucketWithPartitions: Got SparkException (${cause.getClass}): $cause")
          throw cause
        }
        throw e
      case unknown: Throwable =>
        Helpers.log(s"readAsBucketWithPartitions: Got some other kind of exception: $unknown")
        throw unknown
    }
  }

  def readAsBucketWithPartitions(sql_ctx: SQLContext, view: String, numBuckets: Int, location: String,
                                 bucketColumns: util.ArrayList[String],
                                 sortColumns: util.ArrayList[String]): Boolean = {

    Helpers.log(s"readAsBucketWithPartitions v2: free memory before (MB): ${MemoryDiagnostics.getFreeMemoryMB}")

    val result = Retry.retry(5) {
      readAsBucketWithPartitionsInternal(sql_ctx, view, numBuckets, location, bucketColumns, sortColumns)
    }
    Await.result(result, 3 hours)
  }

  private def readAsBucketWithPartitionsInternal(sql_ctx: SQLContext, view: String, numBuckets: Int, location: String,
                                                 bucketColumns: util.ArrayList[String],
                                                 sortColumns: util.ArrayList[String]) = {
    Helpers.log(s"readAsBucketWithPartitions v2: view=$view numBuckets=$numBuckets location=$location "
      + f"bucket_columns(${bucketColumns.size()})=$bucketColumns, sort_columns=$sortColumns")
    try {
      val temp_view = s"${view}_temp_bucket_reader"
      val raw_table_name = s"${view}_raw_buckets"
      sql_ctx.sql(s"DROP TABLE IF EXISTS default.$raw_table_name")
      val sql: String = getCreateTableCommand(sql_ctx, numBuckets, location,
        bucketColumns, sortColumns, temp_view, raw_table_name)
      Helpers.log(sql)
      sql_ctx.sql(sql)
      sql_ctx.sql(s"DROP VIEW $temp_view") // done with view
      Helpers.log(s"REFRESH TABLE default.$raw_table_name")
      sql_ctx.sql(s"REFRESH TABLE default.$raw_table_name")
      val bucketColumnsAsCsv: String = Helpers.getSeqString(bucketColumns).mkString(",")
      Helpers.log(s"ANALYZE TABLE default.$raw_table_name COMPUTE STATISTICS FOR COLUMNS $bucketColumnsAsCsv")
      sql_ctx.sql(s"ANALYZE TABLE default.$raw_table_name COMPUTE STATISTICS FOR COLUMNS $bucketColumnsAsCsv")
      // sql_ctx.sql(s"DESCRIBE EXTENDED $table_name").show(numRows = 1000)
      val result_df = sql_ctx.table(raw_table_name)
      result_df.createOrReplaceTempView(view)
      // sql_ctx.sql(s"SELECT * FROM $view").explain(extended = true)
      Helpers.log(s"readAsBucketWithPartitions: free memory after (MB): ${MemoryDiagnostics.getFreeMemoryMB}")

      true
    }
    catch {
      case e: SparkException =>
        val cause = e.getCause
        if (cause != null) {
          Helpers.log(s"readAsBucketWithPartitions: Got SparkException (${cause.getClass}): $cause")
          throw cause
        }
        throw e
      case e: AnalysisException =>
        // we do this instead of checking if data frame is empty because the latter is expensive
        if (e.message.startsWith(s"cannot resolve '`${bucketColumns.get(0)}`' given input columns") || e.message.startsWith("Unable to infer schema for Parquet. It must be specified manually")) {
          Helpers.log(s"__internalCheckpointBucketWithPartitions: data frame passed in is empty. $e")
          false
        }
        else {
          Helpers.log(s"__internalCheckpointBucketWithPartitions: Got AnalysisException: $e")
          throw e
        }
      case unknown: Throwable =>
        Helpers.log(s"readAsBucketWithPartitions: Got some other kind of exception: $unknown")
        throw unknown
    }
  }

  private def getCreateTableCommand(sql_ctx: SQLContext, numBuckets: Int, location: String,
                                    bucketColumns: util.ArrayList[String],
                                    sortColumns: util.ArrayList[String],
                                    view_for_schema: String,
                                    table_name: String): String = {
    require(bucketColumns.size() > 0, f"There were no bucket columns specified")
    require(sortColumns.size() > 0, f"There were no sort columns specified")
    // get schema from parquet file without loading data from it
    val df = sql_ctx.read.format("parquet")
      .load(location)

    df.createOrReplaceTempView(view_for_schema)
    val columns = _getColumnsSchema(sql_ctx, view_for_schema)
    val bucket_by_text = Helpers.getSeqString(bucketColumns).mkString(",")
    val sort_by_text = Helpers.getSeqString(sortColumns).mkString(",")
    // have to use CREATE TABLE syntax since that supports bucketing
    var text = s"CREATE TABLE $table_name ("
    text += columns.map(column => s"\n${column(0)} ${column(1)}").mkString(",")
    text += ")\n"
    text +=
      s"""
            USING org.apache.spark.sql.parquet
            OPTIONS (
              path "$location"
            )
            CLUSTERED BY ($bucket_by_text) SORTED BY ($sort_by_text) INTO $numBuckets BUCKETS
            """
    text
  }

  private def _getColumnsSchema(sql_ctx: SQLContext, temp_view: String) = {
    val df_schema = sql_ctx.sql(s"DESCRIBE $temp_view")
    _getColumnSchemaFromDataFrame(df_schema)
  }

  private def _getColumnSchemaFromDataFrame(df_schema: DataFrame) = {
    val columns = df_schema.select(col("col_name"), col("data_type"))
      .rdd.map(x => x.toSeq.toArray).collect()
    columns
  }

  def addBucketColumn(sql_ctx: SQLContext, view: String, result_view: String,
                      numBuckets: Int,
                      bucketColumns: util.ArrayList[String],
                      sortColumns: util.ArrayList[String]): Boolean = {
    Helpers.log(f"addBucketColumn v2: Adding bucket column to $view")
    val df: DataFrame = sql_ctx.table(view)

    val result_df: DataFrame = addBucketColumnToDataFrame(df, view, numBuckets, bucketColumns, sortColumns = sortColumns)

    result_df.createOrReplaceTempView(result_view)
    true
  }

  def addBucketColumnToDataFrame(df: DataFrame,
                                 view: String,
                                 numBuckets: Int,
                                 bucketColumns: util.ArrayList[String],
                                 sortColumns: util.ArrayList[String]
                                ): DataFrame = {
    require(bucketColumns.size() > 0, f"There were no bucket columns specified")
    var result_df: DataFrame = df
    val bucketColumnsSeq: Seq[String] = Helpers.getSeqString(bucketColumns)
    val bucketColumnsTypeSeq = bucketColumnsSeq.map(x => col(x))
    val sortColumnsSeq: Seq[String] = Helpers.getSeqString(sortColumns).drop(1)

    Helpers.log(s"Adding bucket column to $view")
    result_df = df
      .repartition(numBuckets, bucketColumnsTypeSeq: _*)
      .sortWithinPartitions(sortColumns.get(0), sortColumnsSeq: _*)

    result_df
  }

}
