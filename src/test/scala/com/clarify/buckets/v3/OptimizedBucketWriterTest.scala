package com.clarify.buckets.v3

import java.io.File
import java.nio.file.{Files, Paths}
import java.util

import com.clarify.checkpoints.v1.CheckPointer
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
      "my_table", 10, location, bucket_columns, sortColumns = bucket_columns, "foo")
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
      view = my_table, numBuckets = 10, location = location,
      bucketColumns = bucket_columns,
      sortColumns = bucket_columns,
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

  test("get latest folder number with prefix over 10") {
    spark.sharedState.cacheManager.clearCache()
    val location = Files.createTempDirectory("hdfs").toFile.toString
    val postfix = CheckPointer.postfix
    create_sample_parquet(location, f"foo${postfix}2")
    create_sample_parquet(location, f"foo${postfix}11")
    val result = CheckPointer.getLatestCheckpointForView(sql_ctx = spark.sqlContext, path = location, view = "foo")
    assert(11 == result)
  }
  test("get latest folder number with no checkpoints") {
    spark.sharedState.cacheManager.clearCache()
    val location = Files.createTempDirectory("hdfs").toFile.toString
    val postfix = CheckPointer.postfix
    create_sample_parquet(location, f"foo2${postfix}2")
    create_sample_parquet(location, f"foo2${postfix}11")
    val result = CheckPointer.getLatestCheckpointForView(sql_ctx = spark.sqlContext, path = location, view = "foo")
    assert(null == result)
  }

  private def create_sample_parquet(location: String, name: String): Unit = {
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
    val folder = Paths.get(location, name)

    df.write.parquet(path = folder.toString)
  }
}
