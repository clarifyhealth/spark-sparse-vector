package com.clarify.checkpoints.v2

import java.util

import com.clarify.Helpers
import com.clarify.buckets.v4.OptimizedBucketWriter.{addBucketColumnToDataFrame, readAsBucketWithPartitions, saveAsBucketWithPartitions}
import com.clarify.hdfs.HdfsHelper
import com.clarify.memory.MemoryDiagnostics
import org.apache.spark.SparkException
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext}

object CheckPointer {
  val postfix: String = "____"

  def __internalCheckpointBucketWithPartitions(sql_ctx: SQLContext, view: String, numBuckets: Int,
                                               location: String,
                                               bucketColumns: util.ArrayList[String],
                                               sortColumns: util.ArrayList[String]): Boolean = {
    Helpers.log(s"__internalCheckpointBucketWithPartitions v3: free memory before (MB): ${MemoryDiagnostics.getFreeMemoryMB}")
    require(bucketColumns.size() > 0, f"There were no bucket columns specified")
    require(sortColumns.size() > 0, f"There were no sort columns specified")
    try {

      val table_prefix = f"temp_${view.toLowerCase()}$postfix"
      // find previous checkpoint tables
      val previous_checkpoint_table_names: Seq[String] =
        sql_ctx.tableNames().filter(x => x.startsWith(table_prefix))
          .sorted.reverse

      println("---tables---")
      sql_ctx.tableNames().foreach(println)
      println("-------------")
      println(f"---- previous_checkpoint_table_names: ${previous_checkpoint_table_names.size} ---")
      previous_checkpoint_table_names.foreach(println)
      println("--------------")

      val previous_checkpoint_numbers: Seq[Int] =
        previous_checkpoint_table_names
          .map(x => x.replace(table_prefix, "").toInt)
          .sorted.reverse

      //      previous_checkpoint_numbers.foreach(println)

      val new_checkpoint_number: Int =
        if (previous_checkpoint_numbers.isEmpty) 1 else previous_checkpoint_numbers.head + 1

      val new_table_name = s"$table_prefix$new_checkpoint_number"

      Helpers.log(s"__internalCheckpointBucketWithPartitions v3: view=$view table=$new_table_name numBuckets=$numBuckets"
        + f" bucket_columns(${bucketColumns.size()})=$bucketColumns, sort_columns=$sortColumns")
      val df: DataFrame = sql_ctx.table(view)

      val my_df: DataFrame = addBucketColumnToDataFrame(df = df, view = view,
        numBuckets = numBuckets, bucketColumns = bucketColumns,
        sortColumns = sortColumns)

      val bucketColumnsSeq: Seq[String] = Helpers.getSeqString(bucketColumns).drop(1)
      val sortColumnsSeq: Seq[String] = Helpers.getSeqString(sortColumns).drop(1)

      my_df
        .write
        .format("parquet")
        //.partitionBy("bucket")
        .bucketBy(numBuckets, colName = bucketColumns.get(0), colNames = bucketColumnsSeq: _*)
        .sortBy(colName = sortColumns.get(0), colNames = sortColumnsSeq: _*)
        .saveAsTable(new_table_name)

      sql_ctx.sql(s"REFRESH TABLE default.$new_table_name")
      // sql_ctx.sql(s"DESCRIBE EXTENDED $new_table_name").show(numRows = 1000)

      // delete all but latest of the previous checkpoints
      if (previous_checkpoint_numbers.nonEmpty) {
        val tables_to_delete: Seq[String] = previous_checkpoint_numbers.drop(1).map(x => f"$table_prefix$x")
        println(f"---- tables to delete: ${tables_to_delete.size} -----")
        tables_to_delete.foreach(println)
        tables_to_delete.foreach(t => {
          println(f"DROP TABLE default.$t")
          sql_ctx.sql(f"DROP TABLE default.$t")
        })
      }
      Helpers.log(s"__internalCheckpointBucketWithPartitions v3: free memory after (MB): ${MemoryDiagnostics.getFreeMemoryMB}")
      val result_df = sql_ctx.table(new_table_name)
      result_df.createOrReplaceTempView(view)

      //      for (tableName <- tableNames.filter(t => t.startsWith(original_table_name))){
      //        Helpers.Helpers.log(s"DROP TABLE default.$tableName")
      //        sql_ctx.sql(s"DROP TABLE default.$tableName")
      //      }

      true
    }
    catch {
      case e: SparkException =>
        val cause = e.getCause
        Helpers.log(s"__internalCheckpointBucketWithPartitions v3: Got SparkException: $cause")
        throw cause
      case e: AnalysisException =>
        // we do this instead of checking if data frame is empty because the latter is expensive
        if (e.message.startsWith(s"cannot resolve '`${bucketColumns.get(0)}`' given input columns")) {
          Helpers.log(s"__internalCheckpointBucketWithPartitions v3: data frame passed in is empty. $e")
          false
        }
        else {
          Helpers.log(s"__internalCheckpointBucketWithPartitions v3: Got AnalysisException: $e")
          throw e
        }
      case unknown: Throwable =>
        Helpers.log(s"__internalCheckpointBucketWithPartitions v3: Got some other kind of exception: $unknown")
        throw unknown
    }
  }

  def __internalCheckpointWithoutBuckets(sql_ctx: SQLContext, view: String, name: String): Boolean = {
    Helpers.log(s"__internalCheckpointWithoutBuckets v4: free memory before (MB): ${MemoryDiagnostics.getFreeMemoryMB}")
    try {

      val table_prefix = f"temp_${view.toLowerCase()}$postfix"
      // find previous checkpoint tables
      val previous_checkpoint_table_names: Seq[String] =
        sql_ctx.tableNames().filter(x => x.startsWith(table_prefix))
          .sorted.reverse

      println("---tables---")
      sql_ctx.tableNames().foreach(println)
      println("-------------")
      println(f"---- previous_checkpoint_table_names: ${previous_checkpoint_table_names.size} ---")
      previous_checkpoint_table_names.foreach(println)
      println("--------------")

      val previous_checkpoint_numbers: Seq[Int] =
        previous_checkpoint_table_names
          .map(x => x.replace(table_prefix, "").toInt)
          .sorted.reverse

      //      previous_checkpoint_numbers.foreach(println)

      val new_checkpoint_number: Int =
        if (previous_checkpoint_numbers.isEmpty) 1 else previous_checkpoint_numbers.head + 1

      val new_table_name = s"$table_prefix$new_checkpoint_number"

      Helpers.log(s"__internalCheckpointWithoutBuckets v4: view=$view table=$new_table_name")
      val df: DataFrame = sql_ctx.table(view)

      val my_df: DataFrame = df

      my_df
        .write
        .format("parquet")
        .saveAsTable(new_table_name)

      sql_ctx.sql(s"REFRESH TABLE default.$new_table_name")
      // sql_ctx.sql(s"DESCRIBE EXTENDED $new_table_name").show(numRows = 1000)

      // delete all but latest of the previous checkpoints
      if (previous_checkpoint_numbers.nonEmpty) {
        val tables_to_delete: Seq[String] = previous_checkpoint_numbers.drop(1).map(x => f"$table_prefix$x")
        println(f"---- tables to delete: ${tables_to_delete.size} -----")
        tables_to_delete.foreach(println)
        tables_to_delete.foreach(t => {
          println(f"DROP TABLE default.$t")
          sql_ctx.sql(f"DROP TABLE default.$t")
        })
      }
      Helpers.log(s"__internalCheckpointWithoutBuckets v4: free memory after (MB): ${MemoryDiagnostics.getFreeMemoryMB}")
      val result_df = sql_ctx.table(new_table_name)
      result_df.createOrReplaceTempView(view)
      true
    }
    catch {
      case e: SparkException =>
        val cause = e.getCause
        Helpers.log(s"__internalCheckpointBucketWithPartitions v3: Got SparkException: $cause")
        throw cause
      case e: AnalysisException =>
        // we do this instead of checking if data frame is empty because the latter is expensive
        Helpers.log(s"__internalCheckpointBucketWithPartitions v3: Got AnalysisException: $e")
        throw e
      case unknown: Throwable =>
        Helpers.log(s"__internalCheckpointBucketWithPartitions v3: Got some other kind of exception: $unknown")
        throw unknown
    }
  }

  def checkpointBucketWithPartitions(sql_ctx: SQLContext,
                                     view: String,
                                     tracking_id: Int,
                                     numBuckets: Int,
                                     location: String,
                                     bucketColumns: util.ArrayList[String],
                                     sortColumns: util.ArrayList[String],
                                     name: String = null): Boolean = {

    if (name != null) {
      sql_ctx.sparkContext.setJobDescription(name)
    }
    Helpers.log(s"checkpointBucketWithPartitions v3 for $view, name=$name, location=$location")
    // if location is specified then use external tables
    if (location != null && location.toLowerCase().startsWith("s3")) {
      _checkpointBucketToDiskInternal(sql_ctx, view, numBuckets, bucketColumns, sortColumns, name, location)
    } else {
      // use Spark managed tables for better performance
      val result = __internalCheckpointBucketWithPartitions(sql_ctx = sql_ctx, view = view,
        numBuckets = numBuckets, location = location,
        bucketColumns = bucketColumns, sortColumns = sortColumns)
      // print free space left
      HdfsHelper._printFreeSpace(sql_ctx.sparkContext)
      result
    }
  }

  def checkpointBucketToDisk(sql_ctx: SQLContext,
                             view: String,
                             tracking_id: Int,
                             numBuckets: Int,
                             location: String,
                             bucketColumns: util.ArrayList[String],
                             sortColumns: util.ArrayList[String],
                             name: String): Boolean = {
    // append name to create a unique location
    val fullLocation = getPathToCheckpoint(view, tracking_id, location)
    _checkpointBucketToDiskInternal(sql_ctx, view, numBuckets, bucketColumns, sortColumns, name, fullLocation)
  }

  private def _checkpointBucketToDiskInternal(sql_ctx: SQLContext, view: String, numBuckets: Int,
                                              bucketColumns: util.ArrayList[String], sortColumns: util.ArrayList[String],
                                              name: String, fullLocation: String): Boolean = {
    Helpers.log(s"checkpointBucketToDisk v3 for $view, name=$name, location=$fullLocation")
    // if folder already exists then just read from it
    if (name != null && HdfsHelper.__folderWithDataExists(sql_ctx, fullLocation, name)) {
      Helpers.log(f"Folder $fullLocation already exists with data so skipping saving table")
      sql_ctx.sparkContext.setJobDescription(f"$name (already exists so reading)")
      readAsBucketWithPartitions(sql_ctx = sql_ctx, view = view, numBuckets = numBuckets,
        location = fullLocation, bucketColumns = bucketColumns, sortColumns = sortColumns)
      return true
    }
    // save to location
    val success = saveAsBucketWithPartitions(sql_ctx = sql_ctx, view = view, numBuckets = numBuckets,
      location = fullLocation, bucketColumns = bucketColumns, sortColumns = sortColumns,
      name = name)
    if (success) {
      // val localLocation = if (location.startsWith("s3:")) f"/tmp/checkpoint/$name" else location
      // val localLocation = location
      // read from location
      if (name != null && HdfsHelper.__folderWithDataExists(sql_ctx, fullLocation, name)) {
        sql_ctx.sparkContext.setJobDescription(f"$name (read after save)")
        readAsBucketWithPartitions(sql_ctx = sql_ctx, view = view, numBuckets = numBuckets,
          location = fullLocation, bucketColumns = bucketColumns,
          sortColumns = sortColumns)
      }
      else {
        // add bucket column to avoid errors
        sql_ctx.table(view).withColumn("bucket", lit(0)).createOrReplaceTempView(view)
        true
      }
    }
    else {
      false
    }
    true
  }

  private def getPathToCheckpoint(view: String, tracking_id: Int, location: String) = {
    HdfsHelper.appendPaths(location, f"$view$postfix$tracking_id").toString
  }

  def checkpointWithoutBuckets(sql_ctx: SQLContext, view: String, numBuckets: Int,
                               location: String,
                               bucketColumns: util.ArrayList[String],
                               sortColumns: util.ArrayList[String],
                               name: String
                              ): Boolean = {

    Helpers.log(s"checkpointWithoutBuckets v3 for $view")
    if (!sql_ctx.table(view).isEmpty) {
      val df = sql_ctx.table(view)
      if (!HdfsHelper.__folderWithDataExists(sql_ctx, location, name)) {
        df.write.parquet(location)
      }
      val result_df = sql_ctx.read.parquet(location)
      result_df.createOrReplaceTempView(view)
      Helpers.log(s"REFRESH TABLE $view")
      sql_ctx.sql(s"REFRESH TABLE $view")
      sql_ctx.sql(s"DESCRIBE EXTENDED $view").show(numRows = 1000)
      true
    }
    else {
      Helpers.log(s"$view was empty so did not bucket it")
      false
    }
  }

  def checkpointManagedTableWithoutBuckets(sql_ctx: SQLContext, view: String,
                                           name: String
                                          ): Boolean = {

    Helpers.log(s"checkpointManagedTableWithoutBuckets v4 for $view")
    __internalCheckpointWithoutBuckets(sql_ctx = sql_ctx, view = view, name = name)
  }


  def checkpointBucketWithPartitionsInMemory(sql_ctx: SQLContext, view: String,
                                             numBuckets: Int, location: String,
                                             bucketColumns: util.ArrayList[String],
                                             sortColumns: util.ArrayList[String]): Boolean = {
    val df = sql_ctx.table(view)
    val rdd = df.rdd
    rdd.cache()
    sql_ctx.createDataFrame(rdd, df.schema).createOrReplaceTempView(view)
    true
  }

  def getLatestCheckpointForView(sql_ctx: SQLContext, path: String, view: String): Any = {
    HdfsHelper.getLatestFolderNumber(sql_ctx.sparkContext, sql_ctx = sql_ctx, path = path, prefix = f"$view$postfix").orNull
  }
}
