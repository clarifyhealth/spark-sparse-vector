package com.clarify.buckets

import java.io.File
import java.nio.file.Files
import java.util

import com.clarify.sparse_vectors.SparkSessionTestWrapper
import com.clarify.{Helpers, TestHelpers}
import org.apache.spark.sql.functions.{col, hash, lit, pmod}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, QueryTest, Row}

class OptimizedBucketWriterTest extends QueryTest with SparkSessionTestWrapper {

  test("save to buckets") {
    spark.sharedState.cacheManager.clearCache()

    val data = List(
      Row(1, "foo"),
      Row(2, "bar"),
      Row(3, "zoo")
    )
    val fields = List(
      StructField("id", IntegerType, nullable = false),
      StructField("v2", StringType, nullable = false))

    val data_rdd = spark.sparkContext.makeRDD(data)

    val df: DataFrame = spark.createDataFrame(data_rdd, StructType(fields))

    df.createOrReplaceTempView("my_table")

    val bucket_columns = new util.ArrayList[String]()
    bucket_columns.add("id")

    val location = Files.createTempDirectory("parquet").toFile.toString
    OptimizedBucketWriter.saveAsBucketWithPartitions(spark.sqlContext,
      "my_table", 10, location, bucket_columns, "foo")
    println(s"Wrote output to: $location")

    println(f"---- files in $location ----")
    Helpers.printRecursiveListOfFiles(new File(location))
    println("-------------------------------")
    // spark.catalog.dropTempView("my_table")

    // now test reading from it
    val result_df: DataFrame = spark.table("my_table")
    result_df.show()

    assert(result_df.count() == df.count())

    TestHelpers.clear_tables(spark_session = spark)
  }

  test("save to buckets multiple rows") {
    spark.sharedState.cacheManager.clearCache()

    val my_table = "my_table_multiple"

    val data = List(
      Row(1, "foo"),
      Row(2, "bar"),
      Row(3, "zoo")
    )
    val fields = List(
      StructField("id", IntegerType, nullable = false),
      StructField("v2", StringType, nullable = false))

    val data_rdd = spark.sparkContext.makeRDD(data)

    val df: DataFrame = spark.createDataFrame(data_rdd, StructType(fields))

    df.createOrReplaceTempView(my_table)

    val bucket_columns = new util.ArrayList[String]()
    bucket_columns.add("id")
    bucket_columns.add("v2")

    val location = Files.createTempDirectory("parquet").toFile.toString
    OptimizedBucketWriter.saveAsBucketWithPartitions(sql_ctx = spark.sqlContext,
      view = my_table, numBuckets = 10, location = location, bucketColumns = bucket_columns,
      name = "bar")
    println(s"Wrote output to: $location")

    val tables = spark.catalog.listTables()
    tables.foreach(t => println(t.name))

    // now test reading from it
    //    OptimizedBucketWriter.readAsBucketWithPartitions2(sql_ctx = spark.sqlContext,
    //      view = my_table, numBuckets = 10, location = location, bucketColumns = bucket_columns)
    val result_df = spark.table(my_table)
    result_df.show()

    assert(result_df.count() == df.count())
    // spark.sql(s"DESCRIBE EXTENDED ${my_table}").show(numRows = 1000, truncate = false)

    TestHelpers.clear_tables(spark_session = spark)
  }

  test("checkpoint to S3 location") {
    spark.sharedState.cacheManager.clearCache()

    val my_table = "my_table_multiple"

    val data = List(
      Row(1, "foo"),
      Row(2, "bar"),
      Row(3, "zoo")
    )
    val fields = List(
      StructField("id", IntegerType, nullable = false),
      StructField("v2", StringType, nullable = false))

    val data_rdd = spark.sparkContext.makeRDD(data)

    val df: DataFrame = spark.createDataFrame(data_rdd, StructType(fields))

    df.createOrReplaceTempView(my_table)

    val bucket_columns = new util.ArrayList[String]()
    bucket_columns.add("id")
    bucket_columns.add("v2")

    val location = Files.createTempDirectory("parquet").toFile.toString
    OptimizedBucketWriter.checkpointBucketToDisk(sql_ctx = spark.sqlContext,
      view = my_table, numBuckets = 10, location = location, bucketColumns = bucket_columns,
      name = "bar")
    println(s"Wrote output to: $location")

    println(f"---- files in $location ----")
    Helpers.printRecursiveListOfFiles(new File(location))
    println("-------------------------------")

    val tables = spark.catalog.listTables()
    tables.foreach(t => println(t.name))

    // now test reading from it
    //    OptimizedBucketWriter.readAsBucketWithPartitions2(sql_ctx = spark.sqlContext,
    //      view = my_table, numBuckets = 10, location = location, bucketColumns = bucket_columns)
    val result_df = spark.table(my_table)
    result_df.show()

    assert(result_df.count() == df.count())
    // spark.sql(s"DESCRIBE EXTENDED ${my_table}").show(numRows = 1000, truncate = false)

    TestHelpers.clear_tables(spark_session = spark)
  }

  test("checkpoint to S3 location multiple times") {
    spark.sharedState.cacheManager.clearCache()

    val my_table = "my_table_multiple"

    val data = List(
      Row(1, "foo"),
      Row(2, "bar"),
      Row(3, "zoo")
    )
    val fields = List(
      StructField("id", IntegerType, nullable = false),
      StructField("v2", StringType, nullable = false))

    val data_rdd = spark.sparkContext.makeRDD(data)

    val df: DataFrame = spark.createDataFrame(data_rdd, StructType(fields))

    df.createOrReplaceTempView(my_table)

    val bucket_columns = new util.ArrayList[String]()
    bucket_columns.add("id")
    bucket_columns.add("v2")

    val location = Files.createTempDirectory("parquet").toFile.toString
    // checkpoint
    OptimizedBucketWriter.checkpointBucketToDisk(sql_ctx = spark.sqlContext,
      view = my_table, numBuckets = 10, location = location, bucketColumns = bucket_columns,
      name = "bar")
    println(s"Wrote output to: $location")

    println(f"---- files in $location ----")
    Helpers.printRecursiveListOfFiles(new File(location))
    println("-------------------------------")

    // checkpoint
    OptimizedBucketWriter.checkpointBucketToDisk(sql_ctx = spark.sqlContext,
      view = my_table, numBuckets = 10, location = location, bucketColumns = bucket_columns,
      name = "bar")
    println(f"---- files in $location ----")
    Helpers.printRecursiveListOfFiles(new File(location))
    println("-------------------------------")

    val tables = spark.catalog.listTables()
    tables.foreach(t => println(t.name))

    // now test reading from it
    //    OptimizedBucketWriter.readAsBucketWithPartitions2(sql_ctx = spark.sqlContext,
    //      view = my_table, numBuckets = 10, location = location, bucketColumns = bucket_columns)
    val result_df = spark.table(my_table)
    result_df.show()

    assert(result_df.count() == df.count())
    // spark.sql(s"DESCRIBE EXTENDED ${my_table}").show(numRows = 1000, truncate = false)

    TestHelpers.clear_tables(spark_session = spark)
  }

  test("checkpoint multiple rows multiple times") {
    spark.sharedState.cacheManager.clearCache()

    val my_table = "my_table_multiple"

    val data = List(
      Row(1, "foo"),
      Row(2, "bar"),
      Row(3, "zoo")
    )
    val fields = List(
      StructField("id", IntegerType, nullable = false),
      StructField("v2", StringType, nullable = false))

    val data_rdd = spark.sparkContext.makeRDD(data)

    val df: DataFrame = spark.createDataFrame(data_rdd, StructType(fields))

    df.createOrReplaceTempView(my_table)

    val bucket_columns = new util.ArrayList[String]()
    bucket_columns.add("id")
    bucket_columns.add("v2")

    val location = Files.createTempDirectory("parquet").toFile.toString
    OptimizedBucketWriter.__internalCheckpointBucketWithPartitions(sql_ctx = spark.sqlContext,
      view = my_table, numBuckets = 10, location = location, bucketColumns = bucket_columns)
    println(s"Wrote output to: $location")

    val tables = spark.catalog.listTables()
    tables.foreach(t => println(t.name))

    // now update the table
    val mid_df: DataFrame = spark.sql(s"select *, 1 as foo from $my_table")
    mid_df.createOrReplaceTempView(my_table)
    // and save again
    OptimizedBucketWriter.__internalCheckpointBucketWithPartitions(sql_ctx = spark.sqlContext,
      view = my_table, numBuckets = 10, location = location, bucketColumns = bucket_columns)
    println(s"Wrote output to: $location")

    // now update the table
    val mid2_df: DataFrame = spark.sql(s"select *, 2 as bar from $my_table")
    mid2_df.createOrReplaceTempView(my_table)
    // and save again
    OptimizedBucketWriter.__internalCheckpointBucketWithPartitions(sql_ctx = spark.sqlContext,
      view = my_table, numBuckets = 10, location = location, bucketColumns = bucket_columns)
    println(s"Wrote output to: $location")

    // now test reading from it
    //    OptimizedBucketWriter.readAsBucketWithPartitions2(sql_ctx = spark.sqlContext,
    //      view = my_table, numBuckets = 10, location = location, bucketColumns = bucket_columns)
    val result_df = spark.table(my_table)
    result_df.show()

    assert(result_df.count() == df.count())
    // spark.sql(s"DESCRIBE EXTENDED ${my_table}").show(numRows = 1000, truncate = false)

    TestHelpers.clear_tables(spark_session = spark)
  }

  test("checkpoint") {
    spark.sharedState.cacheManager.clearCache()

    val my_table = "my_table_checkpoint"

    val data = List(
      Row(1, "foo"),
      Row(2, "bar"),
      Row(3, "zoo")
    )
    val fields = List(
      StructField("id", IntegerType, nullable = false),
      StructField("v2", StringType, nullable = false))

    val data_rdd = spark.sparkContext.makeRDD(data)

    val df: DataFrame = spark.createDataFrame(data_rdd, StructType(fields))

    df.createOrReplaceTempView(my_table)

    val bucket_columns = new util.ArrayList[String]()
    bucket_columns.add("id")
    bucket_columns.add("v2")

    val location = Files.createTempDirectory("parquet").toFile.toString
    val result = OptimizedBucketWriter.checkpointBucketWithPartitions(sql_ctx = spark.sqlContext,
      view = my_table, numBuckets = 10, location = location, bucketColumns = bucket_columns)
    assert(result)
    println(s"Wrote output to: $location")
    val result_df = spark.table(my_table)

    val tables = spark.catalog.listTables()
    tables.foreach(t => println(t.name))

    // now test reading from it
    result_df.show()

    assert(result_df.count() == df.count())
    spark.sql(s"DESCRIBE EXTENDED $my_table").show(numRows = 1000, truncate = false)
    TestHelpers.clear_tables(spark_session = spark)
  }

  test("checkpoint empty data frame") {
    spark.sharedState.cacheManager.clearCache()

    val df: DataFrame = spark.emptyDataFrame

    val my_table = "my_table_empty"
    df.createOrReplaceTempView(my_table)

    val bucket_columns = new util.ArrayList[String]()
    bucket_columns.add("id")

    val location = Files.createTempDirectory("parquet").toFile.toString
    println("my view")
    println(df.count())
    println(df.take(1).isEmpty)
    df.show()
    println(spark.sqlContext.table(my_table) count())
    println(spark.sqlContext.table(my_table).take(1).isEmpty)
    val result = OptimizedBucketWriter.checkpointBucketWithPartitions(sql_ctx = spark.sqlContext,
      view = my_table, numBuckets = 10, location = location, bucketColumns = bucket_columns)
    assert(!result)
    TestHelpers.clear_tables(spark_session = spark)
  }

  test("checkpoint empty data frame two columns") {
    spark.sharedState.cacheManager.clearCache()

    val df: DataFrame = spark.emptyDataFrame

    val my_table = "my_table_empty"
    df.createOrReplaceTempView(my_table)

    val bucket_columns = new util.ArrayList[String]()
    bucket_columns.add("id")
    bucket_columns.add("v2")

    val location = Files.createTempDirectory("parquet").toFile.toString
    println("my view")
    println(df.count())
    println(df.take(1).isEmpty)
    df.show()
    println(spark.sqlContext.table(my_table) count())
    println(spark.sqlContext.table(my_table).take(1).isEmpty)
    val result = OptimizedBucketWriter.checkpointBucketWithPartitions(sql_ctx = spark.sqlContext,
      view = my_table, numBuckets = 10, location = location, bucketColumns = bucket_columns)
    assert(!result)
    TestHelpers.clear_tables(spark_session = spark)
  }

  test("calculate bucket") {
    spark.sharedState.cacheManager.clearCache()

    val data = List(
      Row(82995L, 28668527357L),
      Row(83021L, 2388667058L),
      Row(83038L, 12444295974L),
      Row(83093L, 605438428L)
    )
    val fields = List(
      StructField("memberuid", LongType, nullable = false),
      StructField("claimuid", LongType, nullable = false))

    val data_rdd = spark.sparkContext.makeRDD(data)

    val df: DataFrame = spark.createDataFrame(data_rdd, StructType(fields))

    val numBuckets = 8000
    val my_df: DataFrame = df
      .withColumn("buckethash",
        hash(
          col("memberuid")
        )
      )
      .withColumn("bucket",
        pmod(
          hash(
            col("memberuid")
          ),
          lit(numBuckets)
        )
      )

    my_df.show()
  }
  test("calculate bucket two fields") {
    spark.sharedState.cacheManager.clearCache()

    val data = List(
      Row(82995L, 28668527357L),
      Row(83021L, 2388667058L),
      Row(83038L, 12444295974L),
      Row(83093L, 605438428L)
    )
    val fields = List(
      StructField("memberuid", LongType, nullable = false),
      StructField("claimuid", LongType, nullable = false))

    val data_rdd = spark.sparkContext.makeRDD(data)

    val df: DataFrame = spark.createDataFrame(data_rdd, StructType(fields))

    val numBuckets = 8000
    val my_df: DataFrame = df
      .withColumn("buckethash",
        hash(
          col("memberuid"),
          col("claimuid")
        )
      )
      .withColumn("bucket",
        pmod(
          hash(
            col("memberuid"),
            col("claimuid")
          ),
          lit(numBuckets)
        )
      )

    my_df.show()
  }
}
