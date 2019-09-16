package com.clarify.hdfs

import java.nio.file._

import com.clarify.hdfs.HdfsHelper.appendPaths
import com.clarify.sparse_vectors.SparkSessionTestWrapper
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, QueryTest, Row}

class HdfsHelperTest extends QueryTest with SparkSessionTestWrapper {
  test("get folders with prefix") {
    spark.sharedState.cacheManager.clearCache()
    val location = Files.createTempDirectory("hdfs").toFile.toString
    create_sample_parquet(location, "foo")
    val result: Seq[String] = HdfsHelper.getFoldersWithPrefix(
      sparkContext = spark.sparkContext,
      sql_ctx = spark.sqlContext,
      path = location, prefix = "f")
    print(result)
    assert(List("foo") == result)
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
    val folder = appendPaths(location, name)

    df.write.parquet(path = folder.toString)
  }

  test("get folder numbers with prefix") {
    spark.sharedState.cacheManager.clearCache()
    val location = Files.createTempDirectory("hdfs").toFile.toString
    create_sample_parquet(location, "foo__1")
    create_sample_parquet(location, "foo__2")
    val result: Seq[Int] = HdfsHelper.getFolderNumbersOnly(sparkContext = spark.sparkContext,
      sql_ctx = spark.sqlContext, path = location, prefix = "foo__")
    assert(List(1, 2) == result)
  }
  test("get latest folder number with prefix") {
    spark.sharedState.cacheManager.clearCache()
    val location = Files.createTempDirectory("hdfs").toFile.toString
    create_sample_parquet(location, "foo__1")
    create_sample_parquet(location, "foo__2")
    val result: Option[Int] = HdfsHelper.getLatestFolderNumber(sparkContext = spark.sparkContext,
      sql_ctx = spark.sqlContext, path = location, prefix = "foo__")
    assert(2 == result.getOrElse())
  }
  test("get latest folder number with prefix over 10") {
    spark.sharedState.cacheManager.clearCache()
    val location = Files.createTempDirectory("hdfs").toFile.toString
    create_sample_parquet(location, "foo__2")
    create_sample_parquet(location, "foo__11")
    val result: Option[Int] = HdfsHelper.getLatestFolderNumber(sparkContext = spark.sparkContext,
      sql_ctx = spark.sqlContext, path = location, prefix = "foo__")
    assert(11 == result.getOrElse())
  }
  test("get latest folder number can handle non-numeric") {
    spark.sharedState.cacheManager.clearCache()
    val location = Files.createTempDirectory("hdfs").toFile.toString
    create_sample_parquet(location, "foo__2")
    create_sample_parquet(location, "foo__text")
    create_sample_parquet(location, "foo__11")
    val result: Option[Int] = HdfsHelper.getLatestFolderNumber(sparkContext = spark.sparkContext,
      sql_ctx = spark.sqlContext, path = location, prefix = "foo__")
    assert(11 == result.getOrElse())
  }
  test("get latest folder number creates folder if it doesn't exist") {
    spark.sharedState.cacheManager.clearCache()
    val location = appendPaths(Files.createTempDirectory("hdfs").toFile.toString, "doesnotexist").toString
    val result: Option[Int] = HdfsHelper.getLatestFolderNumber(sparkContext = spark.sparkContext,
      sql_ctx = spark.sqlContext, path = location, prefix = "foo__")
    assert(null == result.orNull)
  }
  ignore("get latest folder number creates folder if it is empty") {
    spark.sharedState.cacheManager.clearCache()
    val data = List[Row](
    )
    val fields = List(
      StructField("id", IntegerType, nullable = false),
      StructField("v2", StringType, nullable = false))

    val data_rdd = spark.sparkContext.makeRDD(data)

    val df: DataFrame = spark.createDataFrame(data_rdd, StructType(fields))
    val location = Files.createTempDirectory("hdfs").toFile.toString
    val folder = appendPaths(location, "foo__3")
    df.show()

    df.write.parquet(path = folder.toString)

    val result: Option[Int] = HdfsHelper.getLatestFolderNumber(sparkContext = spark.sparkContext,
      sql_ctx = spark.sqlContext, path = location, prefix = "foo__")
    assert(null == result.orNull)
  }

  test("check appending paths") {
    val result = appendPaths("s3://tera-feature-lake/checkpoints/medicaid/v1/", "claims____1")
    assert(result == "s3://tera-feature-lake/checkpoints/medicaid/v1/claims____1")
  }
}
