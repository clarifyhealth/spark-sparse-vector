package com.clarify.stats.v1

import com.clarify.TestHelpers
import com.clarify.sparse_vectors.SparkSessionTestWrapper
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, QueryTest, Row}

class StatsCalculatorSlimTest extends QueryTest with SparkSessionTestWrapper {

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
      StatsCalculatorSlim._create_statistics(df, 100, 10,
        Seq(),
        my_table
      )

    println(f"result: ${result.count()}")
    result.show(truncate = false)

    checkAnswer(
      result.selectExpr("column_name", "sample_min", "sample_max", "sample_mean"),
      Seq(
        Row("id", 1.0, 8.0, 4.5),
        Row("name", null, null, null),
        Row("v1", 5.0, 11.0, 6.75)
      )
    )

    TestHelpers.clear_tables(spark_session = spark)
  }
}
