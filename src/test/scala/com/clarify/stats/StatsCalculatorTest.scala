package com.clarify.stats

import com.clarify.TestHelpers
import com.clarify.sparse_vectors.SparkSessionTestWrapper
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, QueryTest, Row}

class StatsCalculatorTest extends QueryTest with SparkSessionTestWrapper {

  test("calculate histogram array for column") {
    spark.sharedState.cacheManager.clearCache()

    val my_table = "my_table_stats"

    val data = List(
      Row(1, "foo", 5),
      Row(2, "foo", 5),
      Row(3, "foo", 6),
      Row(4, "foo", 7),
      Row(5, "foo", 5),
      Row(6, "foo", 9),
      Row(7, "bar", 6),
      Row(8, "zoo", 11)
    )
    val fields = List(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("v1", IntegerType, nullable = false)
    )

    val data_rdd = spark.sparkContext.makeRDD(data)

    val df: DataFrame = spark.createDataFrame(data_rdd, StructType(fields))

    df.createOrReplaceTempView(my_table)

    val normal_columns: Seq[(String, String)] = Seq(("id", "int"), ("name", "string"), ("v1", "int"))
    val columns_to_histogram: Seq[String] = Seq("id", "name", "v1")

    val result: Seq[(String, Int)] =
      StatsCalculator._calculate_histogram_array_for_column("v1", df)

    println(f"result: ${result.size}")
    result.foreach(println)

    val expected: Seq[(String, Int)] = Seq(("5", 3), ("6", 2), ("9", 1), ("7", 1), ("11", 1))
    assert(result == expected)

    TestHelpers.clear_tables(spark_session = spark)
  }
  test("calculate histogram arrays for all columns") {
    spark.sharedState.cacheManager.clearCache()

    val my_table = "my_table_stats"

    val data = List(
      Row(1, "foo", 5),
      Row(2, "foo", 5),
      Row(3, "foo", 6),
      Row(4, "foo", 7),
      Row(5, "foo", 5),
      Row(6, "foo", 9),
      Row(7, "bar", 6),
      Row(8, "zoo", 11)
    )
    val fields = List(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("v1", IntegerType, nullable = false)
    )

    val data_rdd = spark.sparkContext.makeRDD(data)

    val df: DataFrame = spark.createDataFrame(data_rdd, StructType(fields))

    df.createOrReplaceTempView(my_table)

    val normal_columns: Seq[(String, String)] = Seq(("id", "int"), ("name", "string"), ("v1", "int"))
    val columns_to_histogram: Seq[String] = Seq("id", "name", "v1")

    val result: Seq[(String, Seq[(String, Int)])] = StatsCalculator._create_histogram_array(
      columns_to_histogram,
      df)

    println(f"result: ${result.size}")
    result.foreach(x => println(x))

    val expected: Seq[(String, Seq[(String, Int)])] =
      Seq(
        ("id",
          Seq(("1", 1), ("6", 1), ("3", 1), ("5", 1), ("4", 1))
        ),
        ("name",
          Seq(("foo", 6), ("bar", 1), ("zoo", 1))
        ),
        ("v1",
          Seq(("5", 3), ("6", 2), ("9", 1), ("7", 1), ("11", 1))
        )
      )
    assert(result == expected)

    TestHelpers.clear_tables(spark_session = spark)
  }
}
