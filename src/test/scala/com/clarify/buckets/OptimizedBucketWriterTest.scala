package com.clarify.buckets

import java.nio.file.Files

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

    val location = Files.createTempDirectory("parquet").toFile.toString
    new OptimizedBucketWriter().saveAsBucketWithPartitioning(sql_ctx = spark.sqlContext,
      view = "my_table", numBuckets = 10, location = location, "id")
    println(s"Wrote output to: $location")

    // now test reading from it
    val result_df: DataFrame = spark.read.parquet(location)
    result_df.show()

    assert(result_df.count() == df.count())
  }

}
