package com.clarify.stats

import com.clarify.TestHelpers
import com.clarify.sparse_vectors.SparkSessionTestWrapper
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, QueryTest, Row}

class StatsCalculatorTest extends QueryTest with SparkSessionTestWrapper {
  test("calculate stats") {
    spark.sharedState.cacheManager.clearCache()

    val my_table = "my_table_stats"

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

    TestHelpers.clear_tables(spark_session = spark)
  }
}
