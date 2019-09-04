package com.clarify.buckets

import java.util

import org.apache.log4j.Logger
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

  def saveAsBucketWithPartitions(sql_ctx: SQLContext, view: String, numBuckets: Int,
                                 location: String, bucketColumns: util.ArrayList[String]): Boolean = {
    println("saveAsBucketWithPartitions: before")
    _list_free_memory

    try {
      require(bucketColumns.size() == 1 || bucketColumns.size() == 2,
        s"bucketColumns length, ${bucketColumns.size()} , is not supported")

      val logger = Logger.getLogger(getClass.getName)
      println(s"saveAsBucketWithPartitions: view=$view numBuckets=$numBuckets location=$location bucket_columns(${bucketColumns.size()})=$bucketColumns")
      val df: DataFrame = sql_ctx.table(view)

      // this is a total hack for now
      val table_name = s"temp_$view"
      sql_ctx.sql(s"DROP TABLE IF EXISTS default.$table_name")

      if (bucketColumns.size() == 1) {
        var my_df = df
        if (!df.columns.contains("bucket")) {
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
        my_df = my_df.cache()
        my_df
          .write
          .format("parquet")
          .partitionBy("bucket")
          .bucketBy(numBuckets, bucketColumns.get(0))
          .sortBy(bucketColumns.get(0))
          .option("path", location)
          .saveAsTable(table_name)

        my_df.unpersist(true)
        sql_ctx.sql(s"DROP TABLE default.$table_name")
      }
      else if (bucketColumns.size() == 2) {
        var my_df: DataFrame = df
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

        // my_df.select("bucket", bucketColumns.get(0), bucketColumns.get(1)).show(numRows = 1000)

        //        val unique_buckets = my_df.select(col("bucket")).distinct().count()
        //        println(s"saveAsBucketWithPartitions: Number of buckets: $unique_buckets")
        my_df = my_df.cache()
        my_df
          .write
          .format("parquet")
          .partitionBy("bucket")
          .bucketBy(numBuckets, bucketColumns.get(0), bucketColumns.get(1))
          .sortBy(bucketColumns.get(0), bucketColumns.get(1))
          .option("path", location)
          .saveAsTable(table_name)

        my_df.unpersist(true)
        sql_ctx.sql(s"DROP TABLE default.$table_name")
      }

      println("saveAsBucketWithPartitions: after")
      _list_free_memory
      // sql_ctx.tableNames().foreach(println)

      true
    }
    catch {
      case unknown: Throwable =>
        println(s"saveAsBucketWithPartitions: Got some other kind of exception: $unknown")
        throw unknown
    }
  }

  def saveAsBucketWithPartitions2(sql_ctx: SQLContext, view: String, numBuckets: Int,
                                  location: String, bucketColumns: util.ArrayList[String]): Array[Array[Any]] = {
    println("saveAsBucketWithPartitions: before")
    _list_free_memory

    try {
      require(bucketColumns.size() == 1 || bucketColumns.size() == 2,
        s"bucketColumns length, ${bucketColumns.size()} , is not supported")

      val logger = Logger.getLogger(getClass.getName)
      println(s"saveAsBucketWithPartitions: view=$view numBuckets=$numBuckets location=$location bucket_columns(${bucketColumns.size()})=$bucketColumns")
      val df: DataFrame = sql_ctx.table(view)

      // this is a total hack for now
      val table_name = s"temp_$view"
      sql_ctx.sql(s"DROP TABLE IF EXISTS default.$table_name")

      var columns: Array[Array[Any]] = new Array(0)

      if (bucketColumns.size() == 1) {
        var my_df = df
          .withColumn("bucket",
            pmod(
              hash(
                col(bucketColumns.get(0))
              ),
              lit(numBuckets)
            )
          )
          .repartition(numBuckets, col("bucket"))

        val unique_buckets = my_df.select(col("bucket")).distinct().count()
        println(s"saveAsBucketWithPartitions: count: ${my_df.count()}")
        println(s"saveAsBucketWithPartitions: Number of buckets: $unique_buckets")
        my_df = my_df.cache()
        my_df
          .write
          .format("parquet")
          .partitionBy("bucket")
          .bucketBy(numBuckets, bucketColumns.get(0))
          .sortBy(bucketColumns.get(0))
          .option("path", location)
          .saveAsTable(table_name)


        columns = _getColumnSchema(sql_ctx, view)
        columns :+ Array(Array("bucket", "int"))
        println(columns)
        my_df.unpersist(true)
        sql_ctx.sql(s"DROP TABLE default.$table_name")
      }
      else if (bucketColumns.size() == 2) {
        var my_df: DataFrame = df
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

        // my_df.select("bucket", bucketColumns.get(0), bucketColumns.get(1)).show(numRows = 1000)

        //        val unique_buckets = my_df.select(col("bucket")).distinct().count()
        //        println(s"saveAsBucketWithPartitions: Number of buckets: $unique_buckets")
        my_df = my_df.cache()
        my_df
          .write
          .format("parquet")
          .partitionBy("bucket")
          .bucketBy(numBuckets, bucketColumns.get(0), bucketColumns.get(1))
          .sortBy(bucketColumns.get(0), bucketColumns.get(1))
          .option("path", location)
          .saveAsTable(table_name)

        columns = _getColumnSchemaFromDataFrame(my_df.describe())

        my_df.unpersist(true)
        sql_ctx.sql(s"DROP TABLE default.$table_name")
      }

      println("saveAsBucketWithPartitions: after")
      _list_free_memory
      sql_ctx.tableNames().foreach(println)

      columns
    }
    catch {
      case unknown: Throwable =>
        println(s"saveAsBucketWithPartitions: Got some other kind of exception: $unknown")
        throw unknown
    }
  }

  import java.util

  def getSeqString(list: util.ArrayList[String]): Seq[String] = JavaConverters.asScalaIteratorConverter(list.listIterator()).asScala.toSeq

  def readAsBucketWithPartitions(sql_ctx: SQLContext, view: String, numBuckets: Int, location: String, bucketColumns: util.ArrayList[String]): Boolean = {

    println("readAsBucketWithPartitions: before")
    _list_free_memory

    require(bucketColumns.size() == 1 || bucketColumns.size() == 2, s"bucketColumns length, ${bucketColumns.size()} , is not supported")
    println(s"readAsBucketWithPartitions: view=$view numBuckets=$numBuckets location=$location bucket_columns(${bucketColumns.size()})=$bucketColumns")
    val logger = Logger.getLogger(getClass.getName)
    // get schema from parquet file without loading data from it
    val df = sql_ctx.read.format("parquet")
      .load(location)
    val temp_view = s"${view}_temp_bucket_reader"
    df.createOrReplaceTempView(temp_view)
    val columns = _getColumnSchema(sql_ctx, temp_view)
    sql_ctx.sql(s"DROP VIEW $temp_view") // done with view
    readAsBucketWithPartitions2(sql_ctx, view, numBuckets, location, bucketColumns, columns)
  }

  private def readAsBucketWithPartitions2(sql_ctx: SQLContext, view: String, numBuckets: Int, location: String,
                                          bucketColumns: util.ArrayList[String], columns: Array[Array[Any]]) = {
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
      sql_ctx.sql(s"DESCRIBE EXTENDED $raw_table_name").show(numRows = 1000)
      val result_df = sql_ctx.table(raw_table_name)
      result_df.createOrReplaceTempView(view)
      sql_ctx.sql(s"SELECT * FROM $view").explain(extended = true)
      println("readAsBucketWithPartitions: after")
      _list_free_memory

      true
    }
    catch {
      case unknown: Throwable =>
        println(s"readAsBucketWithPartitions: Got some other kind of exception: $unknown")
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

    saveAsBucketWithPartitions(sql_ctx = sql_ctx, view = view, numBuckets = numBuckets, location = location, bucketColumns = bucketColumns)
    val logger = Logger.getLogger(getClass.getName)
    // get schema from parquet file without loading data from it
    val df = sql_ctx.read.format("parquet")
      .load(location)
    val temp_view = s"${view}_temp_bucket_reader"
    df.createOrReplaceTempView(temp_view)
    val columns = _getColumnSchema(sql_ctx, temp_view)
    sql_ctx.sql(s"DROP VIEW $temp_view") // done with view
    readAsBucketWithPartitions2(sql_ctx, view, numBuckets, location, bucketColumns, columns)
    //    val df2 = sql_ctx.read.parquet(location)
    //    df2.createOrReplaceTempView(view)
    sql_ctx.sql(s"DESCRIBE EXTENDED $view").show(numRows = 1000)
    true
  }

  def checkpointBucketWithPartitionsOld1(sql_ctx: SQLContext, view: String, numBuckets: Int,
                                         location: String, bucketColumns: util.ArrayList[String]): Boolean = {

    val columns: Array[Array[Any]] = saveAsBucketWithPartitions2(sql_ctx = sql_ctx, view = view, numBuckets = numBuckets, location = location, bucketColumns = bucketColumns)
    readAsBucketWithPartitions2(sql_ctx, view, numBuckets, location, bucketColumns, columns)
  }

  def checkpointBucketWithPartitionsOld2(sql_ctx: SQLContext, view: String, numBuckets: Int,
                                         location: String, bucketColumns: util.ArrayList[String]): Boolean = {
    sql_ctx.table(view).write.parquet(location)
    val df = sql_ctx.read.parquet(location)
    df.createOrReplaceTempView(view)
    true
  }

  def checkpointBucketWithPartitionsOld(sql_ctx: SQLContext, view: String, numBuckets: Int,
                                        location: String, bucketColumns: util.ArrayList[String]): Boolean = {
    saveAsBucketWithPartitions(sql_ctx = sql_ctx, view = view, numBuckets = numBuckets, location = location, bucketColumns = bucketColumns)
    readAsBucketWithPartitions(sql_ctx = sql_ctx, view = view, numBuckets = numBuckets, location = location, bucketColumns = bucketColumns)
  }


  def checkpointBucketWithPartitionsInMemory(sql_ctx: SQLContext, view: String, numBuckets: Int, location: String, bucketColumns: util.ArrayList[String]): Boolean = {
    val df = sql_ctx.table(view)
    val rdd = df.rdd
    rdd.cache()
    sql_ctx.createDataFrame(rdd, df.schema).createOrReplaceTempView(view)
    true
    //    saveAsBucketWithPartitions(sql_ctx = sql_ctx, view = view, numBuckets = numBuckets, location = location, bucketColumns = bucketColumns)
    //    readAsBucketWithPartitions(sql_ctx = sql_ctx, view = view, numBuckets = numBuckets, location = location, bucketColumns = bucketColumns)
  }

  def _list_free_memory(): Unit = {
    // memory info
    val mb = 1024 * 1024
    val runtime = Runtime.getRuntime
    println("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb)
    println("** Free Memory:  " + runtime.freeMemory / mb)
    println("** Total Memory: " + runtime.totalMemory / mb)
    println("** Max Memory:   " + runtime.maxMemory / mb)
  }
}
