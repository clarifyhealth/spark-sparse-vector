package com.clarify.hdfs

import java.nio.file._

import com.clarify.sparse_vectors.SparkSessionTestWrapper
import org.apache.spark.sql.QueryTest

class HdfsHelperTest extends QueryTest with SparkSessionTestWrapper {
  test("get folders with prefix") {
    spark.sharedState.cacheManager.clearCache()
    val location = Files.createTempDirectory("hdfs").toFile.toString
    val folder = Paths.get(location, "foo")
    Files.createDirectory(folder)
    val result: Seq[String] = HdfsHelper.getFoldersWithPrefix(sparkContext = spark.sparkContext, path = location, prefix = "f")
    print(result)
    assert(List("foo") == result)
  }
  test("get folder numbers with prefix") {
    spark.sharedState.cacheManager.clearCache()
    val location = Files.createTempDirectory("hdfs").toFile.toString
    Files.createDirectory(Paths.get(location, "foo__1"))
    Files.createDirectory(Paths.get(location, "foo__2"))
    val result: Seq[Int] = HdfsHelper.getFolderNumbersOnly(sparkContext = spark.sparkContext, path = location, prefix = "foo__")
    assert(List(1, 2) == result)
  }
  test("get latest folder number with prefix") {
    spark.sharedState.cacheManager.clearCache()
    val location = Files.createTempDirectory("hdfs").toFile.toString
    Files.createDirectory(Paths.get(location, "foo__1"))
    Files.createDirectory(Paths.get(location, "foo__2"))
    val result: Option[Int] = HdfsHelper.getLatestFolderNumber(sparkContext = spark.sparkContext, path = location, prefix = "foo__")
    assert(2 == result.getOrElse())
  }
  test("get latest folder number with prefix over 10") {
    spark.sharedState.cacheManager.clearCache()
    val location = Files.createTempDirectory("hdfs").toFile.toString
    Files.createDirectory(Paths.get(location, "foo__2"))
    Files.createDirectory(Paths.get(location, "foo__11"))
    val result: Option[Int] = HdfsHelper.getLatestFolderNumber(sparkContext = spark.sparkContext, path = location, prefix = "foo__")
    assert(11 == result.getOrElse())
  }
}
