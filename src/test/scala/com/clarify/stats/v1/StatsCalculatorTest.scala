package com.clarify.stats.v1

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

    val result: Seq[(String, Double)] =
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

    val result: Seq[(String, Seq[(String, Double)])] = StatsCalculator._create_histogram_array(
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
  test("calculate statistics") {
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

    val columns_to_histogram: Seq[String] = Seq("id", "name", "v1")

    val result: DataFrame =
      StatsCalculator._create_statistics(df, 100, 10,
        Seq(),
        columns_to_histogram, my_table
      )

    println(f"result: ${result.count()}")
    result.show(truncate = false)

    checkAnswer(
      result.selectExpr("column_name", "sample_count_distinct", "sample_min", "top_value_1", "top_value_percent_1"),
      Seq(
        Row("id", 8, 1, "1", 10.0),
        Row("name", 3, null, "foo", 60.0),
        Row("v1", 5, 5, "5", 30.0)
      )
    )

    TestHelpers.clear_tables(spark_session = spark)
  }
}
