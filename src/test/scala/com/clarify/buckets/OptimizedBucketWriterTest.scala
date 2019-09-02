package com.clarify.buckets

import java.nio.file.Files
import java.util

import com.clarify.sparse_vectors.SparkSessionTestWrapper
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
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
    OptimizedBucketWriter.saveAsBucketWithPartitions(sql_ctx = spark.sqlContext,
      view = "my_table", numBuckets = 10, location = location, bucketColumns = bucket_columns)
    println(s"Wrote output to: $location")

    spark.catalog.dropTempView("my_table")

    // now test reading from it
    val result_df: DataFrame = spark.read.parquet(location)
    result_df.show()

    assert(result_df.count() == df.count())
  }

  test("save to buckets multiple") {
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

    df.createOrReplaceTempView("my_table_multiple")

    val bucket_columns = new util.ArrayList[String]()
    bucket_columns.add("id")
    bucket_columns.add("v2")

    val location = Files.createTempDirectory("parquet").toFile.toString
    OptimizedBucketWriter.saveAsBucketWithPartitions(sql_ctx = spark.sqlContext,
      view = "my_table_multiple", numBuckets = 10, location = location, bucketColumns = bucket_columns)
    println(s"Wrote output to: $location")

    val tables = spark.catalog.listTables()
    tables.foreach(t => println(t.name))

    spark.catalog.dropTempView("my_table_multiple")
    // now test reading from it
    val result_df = OptimizedBucketWriter.readAsBucketWithPartitions(sql_ctx = spark.sqlContext,
      view = "my_table_multiple2", numBuckets = 10, location = location, bucketColumns = bucket_columns)
    result_df.show()

    assert(result_df.count() == df.count())
    spark.sql(s"DESCRIBE EXTENDED my_table_multiple2").show(numRows = 1000, truncate = false)
  }

  test("checkpoint") {
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

    df.createOrReplaceTempView("my_table_multiple")

    val bucket_columns = new util.ArrayList[String]()
    bucket_columns.add("id")
    bucket_columns.add("v2")

    val location = Files.createTempDirectory("parquet").toFile.toString
    val result_df = OptimizedBucketWriter.checkpointBucketWithPartitions(sql_ctx = spark.sqlContext,
      view = "my_table_multiple", numBuckets = 10, location = location, bucketColumns = bucket_columns)
    println(s"Wrote output to: $location")

    val tables = spark.catalog.listTables()
    tables.foreach(t => println(t.name))

    // now test reading from it
    result_df.show()

    assert(result_df.count() == df.count())
    spark.sql(s"DESCRIBE EXTENDED my_table_multiple").show(numRows = 1000, truncate = false)
  }
}