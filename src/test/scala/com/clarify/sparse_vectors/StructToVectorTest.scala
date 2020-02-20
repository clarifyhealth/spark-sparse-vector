package com.clarify.sparse_vectors

import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, QueryTest, Row}


class StructToVectorTest extends QueryTest with SparkSessionTestWrapper {

  test("struct to vector simple") {
    spark.sharedState.cacheManager.clearCache()
    val schema = StructType(
      StructField("type", ByteType, false) ::
        StructField("size", IntegerType, false) ::
        StructField("indices", ArrayType(IntegerType, false), false) ::
        StructField("values", ArrayType(DoubleType, false), false) ::
        Nil)
    val values = Array(0.toByte, 3, Array(0, 1, 2).toSeq, Array(3.0, 4.0, 5.0).toSeq)
    val v1 = new GenericRowWithSchema(values, schema)
    val v3 = new StructToVector().convert_struct_to_vector(v1)
    assert(v3 == new SparseVector(3, Array(0, 1, 2), Array(3.0, 4.0, 5.0)))
  }
  test("struct to vector data frame sparse vector") {
    spark.sharedState.cacheManager.clearCache()

    val rdd = spark.sparkContext.parallelize(
      Row(0.toByte, 2, List(0, 1).toArray, List(1.0, 2.0).toArray) ::
        Row(0.toByte, 3, List(0, 1, 2).toArray, List(3.0, 4.0, 5.0).toArray) ::
        Nil)

    val schema = StructType(
      StructField("type", ByteType, false) ::
        StructField("size", IntegerType, false) ::
        StructField("indices", ArrayType(IntegerType, false), false) ::
        StructField("values", ArrayType(DoubleType, false), false) ::
        Nil)

    val df: DataFrame = spark.createDataFrame(rdd, schema).select(struct("type", "size", "indices", "values").alias("v1"))

    df.printSchema()
    df.show()
    df.createOrReplaceTempView("my_table2")
    val convert_struct_to_vector_function = new StructToVector().call _

    spark.udf.register("convert_struct_to_vector", convert_struct_to_vector_function)

    val out_df = spark.sql(
      "select convert_struct_to_vector(v1) as result from my_table2"
    )

    out_df.show(truncate = false)
    out_df.printSchema()

    checkAnswer(
      out_df.selectExpr("result"),
      Seq(
        Row(new SparseVector(2, Array(0, 1), Array(1.0, 2.0))),
        Row(new SparseVector(3, Array(0, 1, 2), Array(3.0, 4.0, 5.0)))
      )
    )
    assert(2 == out_df.count())
  }
  test("struct to vector data frame sparse and dense vectors") {
    spark.sharedState.cacheManager.clearCache()

    val rdd = spark.sparkContext.parallelize(
      Row(0.toByte, 2, List(0, 1).toArray, List(1.0, 2.0).toArray) ::
        Row(1.toByte, 0, Nil, List(3.0, 4.0, 5.0).toArray) ::
        Nil)

    val schema = StructType(
      StructField("type", ByteType, false) ::
        StructField("size", IntegerType, false) ::
        StructField("indices", ArrayType(IntegerType, false), false) ::
        StructField("values", ArrayType(DoubleType, false), false) ::
        Nil)

    val df: DataFrame = spark.createDataFrame(rdd, schema).select(struct("type", "size", "indices", "values").alias("v1"))

    df.printSchema()
    df.show()
    df.createOrReplaceTempView("my_table2")
    val convert_struct_to_vector_function = new StructToVector().call _

    spark.udf.register("convert_struct_to_vector", convert_struct_to_vector_function)

    val out_df = spark.sql(
      "select convert_struct_to_vector(v1) as result from my_table2"
    )

    out_df.show(truncate = false)
    out_df.printSchema()

    checkAnswer(
      out_df.selectExpr("result"),
      Seq(
        Row(new SparseVector(2, Array(0, 1), Array(1.0, 2.0))),
        Row(new DenseVector(Array(3.0, 4.0, 5.0)))
      )
    )
    assert(2 == out_df.count())
  }
}
