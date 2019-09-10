package com.clarify.buckets

import java.util

import com.clarify.Helpers
import com.clarify.memory.MemoryDiagnostics
import org.apache.spark.sql.functions.{col, hash, lit, pmod}
import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkException}
import org.slf4j.{Logger, LoggerFactory}

import scala.sys.process._

object OptimizedBucketWriter {

  val _LOGGER: Logger = LoggerFactory.getLogger(this.getClass.getName)

  def saveAsBucketWithPartitions(sql_ctx: SQLContext, view: String, numBuckets: Int,
                                 location: String, bucketColumns: util.ArrayList[String],
                                 name: String): Boolean = {
    Helpers.log(s"saveAsBucketWithPartitions: free memory before (MB): ${MemoryDiagnostics.getFreeMemoryMB}")
    require(location != null, "location cannot be null")
    require(numBuckets > 0, f"numBuckets $numBuckets should be greater than 0")
    require(bucketColumns.size() == 1 || bucketColumns.size() == 2,
      s"bucketColumns length, ${bucketColumns.size()} , is not supported.  We only support 1 and 2 right now.")

    // if folder exists then skip writing
    if (name != null && __folderWithDataExists(sql_ctx, location)) {
      Helpers.log(f"Folder $location already exists with data so skipping saving table")
      return true
    }

    _saveBucketsInternal(sql_ctx, view, numBuckets, location, bucketColumns, name, saveLocalAndCopyToS3 = false)
  }

  private def _saveBucketsInternal(sql_ctx: SQLContext, view: String, numBuckets: Int,
                                   location: String, bucketColumns: util.ArrayList[String],
                                   name: String,
                                   saveLocalAndCopyToS3: Boolean): Boolean = {

    // To avoid S3 slowdown due to writing too many files, write to local and then copy to s3
    val localLocation = if (saveLocalAndCopyToS3 && location.startsWith("s3:")) f"/tmp/checkpoint/$name" else location
    try {
      if (name != null) {
        sql_ctx.sparkContext.setJobDescription(name)
      }
      Helpers.log(s"saveAsBucketWithPartitions: view=$view numBuckets=$numBuckets location=$location"
        + f" bucket_columns(${bucketColumns.size()})=$bucketColumns")
      val df: DataFrame = sql_ctx.table(view)

      val table_name = s"temp_$view"
      sql_ctx.sql(s"DROP TABLE IF EXISTS default.$table_name")

      if (bucketColumns.size() == 1) {
        var my_df = df
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

        my_df
          .write
          .format("parquet")
          .partitionBy("bucket")
          .bucketBy(numBuckets, bucketColumns.get(0))
          .sortBy(bucketColumns.get(0))
          .option("path", localLocation)
          .saveAsTable(table_name)

        Helpers.log(s"DROP TABLE default.$table_name")
        sql_ctx.sql(s"DROP TABLE default.$table_name")
      }
      else if (bucketColumns.size() == 2) {
        var my_df = df
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

        my_df
          .write
          .format("parquet")
          .partitionBy("bucket")
          .bucketBy(numBuckets, bucketColumns.get(0), bucketColumns.get(1))
          .sortBy(bucketColumns.get(0), bucketColumns.get(1))
          .option("path", localLocation)
          .saveAsTable(table_name)

        Helpers.log(s"DROP TABLE default.$table_name")
        sql_ctx.sql(s"DROP TABLE default.$table_name")
      }

      if (saveLocalAndCopyToS3 && location.startsWith("s3:")) {
        Helpers.log(f"s3-dist-cp --s3Endpoint=s3.us-west-2.amazonaws.com --src=hdfs://$localLocation --dest=$location")
        val results = Seq("s3-dist-cp",
          "--s3Endpoint=s3.us-west-2.amazonaws.com",
          f"--src=hdfs://$localLocation",
          f"--dest=$location").!!.trim
        Helpers.log(results)
      }
      Helpers.log(s"saveAsBucketWithPartitions: free memory after (MB): ${MemoryDiagnostics.getFreeMemoryMB}")

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
      case e: AnalysisException =>
        // we do this instead of checking if data frame is empty because the latter is expensive
        if (e.message.startsWith(s"cannot resolve '`${bucketColumns.get(0)}`' given input columns")) {
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

  private def _getColumnSchema(sql_ctx: SQLContext, temp_view: String) = {
    val df_schema = sql_ctx.sql(s"DESCRIBE $temp_view")
    _getColumnSchemaFromDataFrame(df_schema)
  }

  private def _getColumnSchemaFromDataFrame(df_schema: DataFrame) = {
    val columns = df_schema.select(col("col_name"), col("data_type"))
      .rdd.map(x => x.toSeq.toArray).collect()
    columns
  }

  def __internalCheckpointBucketWithPartitions(sql_ctx: SQLContext, view: String, numBuckets: Int,
                                               location: String, bucketColumns: util.ArrayList[String]): Boolean = {
    Helpers.log(s"__internalCheckpointBucketWithPartitions: free memory before (MB): ${MemoryDiagnostics.getFreeMemoryMB}")

    try {
      require(bucketColumns.size() == 1 || bucketColumns.size() == 2,
        s"bucketColumns length, ${bucketColumns.size()} , is not supported.  We only support 1 and 2 right now.")

      val postfix: String = "____"

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

      Helpers.log(s"__internalCheckpointBucketWithPartitions: view=$view table=$new_table_name numBuckets=$numBuckets"
        + f" bucket_columns(${bucketColumns.size()})=$bucketColumns")
      val df: DataFrame = sql_ctx.table(view)

      // val original_table_name = s"temp_$view"
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
        //        Helpers.Helpers.log(s"saveAsBucketWithPartitions: count: ${my_df.count()}")
        //        Helpers.Helpers.log(s"saveAsBucketWithPartitions: Number of buckets: $unique_buckets")
        //        Helpers.Helpers.log(s"Caching df for $view")
        //        my_df = my_df.cache()
        //        Helpers.Helpers.log(s"Finished caching df for $view")


        //        my_df.unpersist(true)
        //        Helpers.Helpers.log(s"REFRESH TABLE default.$original_table_name")
        //        sql_ctx.sql(s"REFRESH TABLE default.$original_table_name")
        //        Helpers.Helpers.log(s"DROP TABLE default.$original_table_name")
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
        //        Helpers.Helpers.log(s"saveAsBucketWithPartitions: Number of buckets: $unique_buckets")
        //        Helpers.Helpers.log(s"Caching df for $view")
        //        my_df = my_df.cache()
        //        Helpers.Helpers.log(s"Finished caching df for $view")
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
        //        Helpers.Helpers.log(s"REFRESH TABLE default.$original_table_name")
        //        sql_ctx.sql(s"REFRESH TABLE default.$original_table_name")
        //        Helpers.Helpers.log(s"DROP TABLE default.$original_table_name")
        //        sql_ctx.sql(s"DROP TABLE default.$original_table_name")
        // sql_ctx.sql(s"DROP TABLE IF EXISTS default.$original_table_name")
      }

      sql_ctx.sql(s"REFRESH TABLE $new_table_name")
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
      Helpers.log(s"__internalCheckpointBucketWithPartitions: free memory after (MB): ${MemoryDiagnostics.getFreeMemoryMB}")
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
        Helpers.log(s"__internalCheckpointBucketWithPartitions: Got SparkException: $cause")
        throw cause
      case e: AnalysisException =>
        // we do this instead of checking if data frame is empty because the latter is expensive
        if (e.message.startsWith(s"cannot resolve '`${bucketColumns.get(0)}`' given input columns")) {
          Helpers.log(s"__internalCheckpointBucketWithPartitions: data frame passed in is empty. $e")
          false
        }
        else {
          Helpers.log(s"__internalCheckpointBucketWithPartitions: Got AnalysisException: $e")
          throw e
        }
      case unknown: Throwable =>
        Helpers.log(s"__internalCheckpointBucketWithPartitions: Got some other kind of exception: $unknown")
        throw unknown
    }
  }

  def checkpointBucketWithPartitions(sql_ctx: SQLContext, view: String, numBuckets: Int,
                                     location: String, bucketColumns: util.ArrayList[String],
                                     name: String = null): Boolean = {

    if (name != null) {
      sql_ctx.sparkContext.setJobDescription(name)
    }
    Helpers.log(s"checkpointBucketWithPartitions for $view, name=$name, location=$location")
    // if location is specified then use external tables
    if (location != null && location.toLowerCase().startsWith("s3")) {
      checkpointToS3(sql_ctx, view, numBuckets, location, bucketColumns, name)
    } else {
      // use Spark managed tables for better performance
      val result = __internalCheckpointBucketWithPartitions(sql_ctx = sql_ctx, view = view,
        numBuckets = numBuckets, location = location, bucketColumns = bucketColumns)
      // print free space left
      _printFreeSpace(sql_ctx.sparkContext)
      result
    }
  }

  def checkpointToS3(sql_ctx: SQLContext, view: String, numBuckets: Int,
                     location: String, bucketColumns: util.ArrayList[String],
                     name: String): Boolean = {
    // append name to create a unique location
    val fullLocation = if (location.endsWith("/")) f"$location$name" else f"$location/$name"
    // if folder already exists then just read from it
    if (name != null && __folderWithDataExists(sql_ctx, location)) {
      Helpers.log(f"Folder $location already exists with data so skipping saving table")
      readAsBucketWithPartitions(sql_ctx = sql_ctx, view = view, numBuckets = numBuckets,
        location = fullLocation, bucketColumns = bucketColumns)
      return true
    }
    // save to location
    val success = saveAsBucketWithPartitions(sql_ctx = sql_ctx, view = view, numBuckets = numBuckets,
      location = fullLocation, bucketColumns = bucketColumns, name)
    if (success) {
      // val localLocation = if (location.startsWith("s3:")) f"/tmp/checkpoint/$name" else location
      val localLocation = location
      // read from location
      readAsBucketWithPartitions(sql_ctx = sql_ctx, view = view, numBuckets = numBuckets,
        location = localLocation, bucketColumns = bucketColumns)
    }
    else {
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

  def _printFreeSpace(sparkContext: SparkContext): Boolean = {
    val deployMode: String = sparkContext.getConf.get("spark.submit.deployMode", null)
    if (deployMode != null && deployMode != "client") {
      //noinspection SpellCheckingInspection
      val results = Seq("hdfs", "dfs", "-df", "-h").!!.trim
      Helpers.log(results)
    }
    else {
      Helpers.log("Skipped showing free space since running in client mode")
    }
  }

  def __folderWithDataExists(sql_ctx: SQLContext, location: String): Boolean = {
    try {
      sql_ctx.read.parquet(location).take(1)
      true
    }
    catch {
      case e: SparkException =>
        val cause = e.getCause
        Helpers.log(s"__folderExists: Got SparkException: $cause")
        throw cause
      case e: AnalysisException =>
        // we do this instead of checking if data frame is empty because the latter is expensive
        if (e.message.startsWith(s"Unable to infer schema for Parquet. It must be specified manually.")) {
          Helpers.log(s"__folderExists: data frame passed in is empty $location. $e")
          false
        }
        else if (e.message.startsWith("Path does not exist")) {
          Helpers.log(s"__folderExists: path does not exist $location. $e")
          false
        }
        else {
          Helpers.log(s"__folderExists: Got AnalysisException: $e")
          throw e
        }
      case unknown: Throwable =>
        Helpers.log(s"__folderExists: Got some other kind of exception: $unknown")
        throw unknown
    }
  }

  def s3distCp(src: String, dest: String): Unit = {
    s"s3-dist-cp --src $src --dest $dest".!
  }
}
