package com.clarify.zipper

import java.util

import com.clarify.TestHelpers
import com.clarify.sparse_vectors.SparkSessionTestWrapper
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, QueryTest, Row}

class DataFrameZipperTest extends QueryTest with SparkSessionTestWrapper {
  test("zipDataFrames") {
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

    TestHelpers.clear_tables(spark_session = spark)
  }

  test("zipDataFramesList") {
    spark.sharedState.cacheManager.clearCache()

    val data1 = List(
      Row(1, "foo"),
      Row(2, "bar"),
      Row(3, "zoo")
    )
    val fields1 = List(
      StructField("id", IntegerType, nullable = false),
      StructField("v2", StringType, nullable = false))
    val data_rdd1 = spark.sparkContext.makeRDD(data1)
    val df1: DataFrame = spark.createDataFrame(data_rdd1, StructType(fields1))
    val table1 = "my_table"
    df1.createOrReplaceTempView(table1)

    val data2 = List(
      Row(1, "foo2"),
      Row(2, "bar2"),
      Row(3, "zoo2")
    )
    val fields2 = List(
      StructField("id", IntegerType, nullable = false),
      StructField("v3", StringType, nullable = false))
    val data_rdd2 = spark.sparkContext.makeRDD(data2)
    val df2: DataFrame = spark.createDataFrame(data_rdd2, StructType(fields2))
    val table2 = "my_table2"
    df2.createOrReplaceTempView(table2)

    val data3 = List(
      Row(1, "foo3"),
      Row(2, "bar3"),
      Row(3, "zoo3")
    )
    val fields3 = List(
      StructField("id", IntegerType, nullable = false),
      StructField("v4", StringType, nullable = false))
    val data_rdd3 = spark.sparkContext.makeRDD(data3)
    val df3: DataFrame = spark.createDataFrame(data_rdd3, StructType(fields3))
    val table3 = "my_table3"
    df3.createOrReplaceTempView(table3)

    val views_to_zip = new util.ArrayList[String]()
    views_to_zip.add(table2)
    views_to_zip.add(table3)

    //val result_table = "result_table"
    DataFrameZipper.zipDataFramesList(df1.sqlContext, table1, views_to_zip, table1, 1, 1, 1)

    // now test reading from it
    val result_df: DataFrame = spark.table(table1)
    result_df.show()

    assert(result_df.count() == df1.count())

    checkAnswer(
      result_df,
      Seq(
        Row(1, "foo", "foo2", "foo3"),
        Row(2, "bar", "bar2", "bar3"),
        Row(3, "zoo", "zoo2", "zoo3")
      )
    )
    TestHelpers.clear_tables(spark_session = spark)
  }
}

