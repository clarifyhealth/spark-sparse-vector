package com.clarify.zipper

import com.clarify.sparse_vectors.SparkSessionTestWrapper
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, QueryTest, Row}

class DataFrameZipperTest extends QueryTest with SparkSessionTestWrapper {
  test("zip") {
    spark.sharedState.cacheManager.clearCache()

    val data1 = List(
      Row(1, "foo"),
      Row(2, "bar"),
      Row(3, "zoo")
    )
    val fields1 = List(
      StructField("id", IntegerType, nullable = false),
      StructField("v2", StringType, nullable = false))
    val data2 = List(
      Row(1, "foo2"),
      Row(2, "bar2"),
      Row(3, "zoo2")
    )
    val fields2 = List(
      StructField("id", IntegerType, nullable = false),
      StructField("v3", StringType, nullable = false))

    val data_rdd1 = spark.sparkContext.makeRDD(data1)

    val data_rdd2 = spark.sparkContext.makeRDD(data2)

    val df1: DataFrame = spark.createDataFrame(data_rdd1, StructType(fields1))
    val df2: DataFrame = spark.createDataFrame(data_rdd2, StructType(fields2))

    val table1 = "my_table"
    val table2 = "my_table2"
    df1.createOrReplaceTempView(table1)
    df2.createOrReplaceTempView(table2)

    //val result_table = "result_table"
    DataFrameZipper.zipDataFrames(df1.sqlContext, table1, table2, table1)

    // now test reading from it
    val result_df: DataFrame = spark.table(table1)
    result_df.show()

    assert(result_df.count() == df1.count())

  }
}
