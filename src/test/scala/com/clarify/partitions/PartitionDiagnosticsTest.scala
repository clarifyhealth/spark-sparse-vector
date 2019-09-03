package com.clarify.partitions

import com.clarify.sparse_vectors.SparkSessionTestWrapper
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, QueryTest, Row}

class PartitionDiagnosticsTest extends QueryTest with SparkSessionTestWrapper {

  test("get partitions") {
    spark.sharedState.cacheManager.clearCache()

    val data = List(
      Row(1, "foo"),
      Row(2, "bar"),
      Row(3, "zoo"),
      Row(4, "zoo"),
      Row(5, "zoo"),
      Row(6, "zoo"),
      Row(7, "zoo"),
      Row(8, "zoo"),
      Row(9, "zoo"),
      Row(10, "zoo"),
      Row(11, "zoo"),
      Row(12, "zoo"),
      Row(13, "zoo"),
      Row(14, "zoo")
    )
    val fields = List(
      StructField("id", IntegerType, nullable = false),
      StructField("v2", StringType, nullable = false))

    val data_rdd = spark.sparkContext.makeRDD(data)

    var df: DataFrame = spark.createDataFrame(data_rdd, StructType(fields))
    df = df.repartition(5, col("id"))

    df.createOrReplaceTempView("my_table")

    val result_df: DataFrame = PartitionDiagnostics.getPartitionsAndCount(df.sqlContext,
      "my_table", "my_table_partitions")

    result_df.show(numRows = 1000)

    assert(result_df.select(sum(col("size"))).collect()(0)(0) == 14.0)
  }
}
