package com.clarify.stats.slim

import com.clarify.TestHelpers
import com.clarify.sparse_vectors.SparkSessionTestWrapper
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, QueryTest, Row}

class StatsCalculatorTest extends QueryTest with SparkSessionTestWrapper {

  test("calculate metrics for table") {
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
    val columns_to_include: Seq[String] = Seq("id", "name", "v1")

    val result: DataFrame =
      StatsCalculator._create_statistics("v1", df)

    println(f"result: ${result.count()}")
    result.collect().foreach(println)

    val result_seq: Seq[(String, Double)] = result.collect().map(row => (row.getString(0), row.getDouble(1))).toSeq

    val expected: Seq[(String, Double)] = Seq(("5", 3.0), ("6", 2.0), ("9", 1.0), ("7", 1.0), ("11", 1.0))
    assert(result_seq == expected)

    TestHelpers.clear_tables(spark_session = spark)
  }
  test("calculate stats for all columns") {
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
    val columns_to_include: Seq[String] = Seq("id", "name", "v1")

    val result: Seq[(String, DataFrame)] = StatsCalculator._create_statistics(
      columns_to_include,
      df)

    println(f"result: ${result.size}")
    result.foreach(x => println(x))
    val result_seq: Seq[(String, Seq[(String, Double)])] = result.map(
      a => (
        a._1,
        a._2.collect().map(
          row => (row.getString(0), row.getDouble(1))
        ).toSeq
      )
    )
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
    assert(result_seq == expected)

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

    val _create_statistics: Seq[String] = Seq("id", "name", "v1")

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
