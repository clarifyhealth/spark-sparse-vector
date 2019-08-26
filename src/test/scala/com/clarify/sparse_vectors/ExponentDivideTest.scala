package com.clarify.sparse_vectors

import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{QueryTest, Row}

class ExponentDivideTest extends QueryTest with SparkSessionTestWrapper {

  test("exponent divide simple") {
    val v1 = new SparseVector(3, Array(0), Array(0.2))
    val v2 = new SparseVector(3, Array(0), Array(0.1))
    val v3 = new ExponentDivide().sparse_vector_exponent_divide(v1, v2)
    assert(v3 == new SparseVector(3, Array(0, 1, 2), Array(1.1051709180756475, 1, 1)))
  }
  test("exponent divide") {
    spark.sharedState.cacheManager.clearCache()

    val data = List(
      Row(new SparseVector(3, Array(0, 2), Array(0.1, 0.2)), new SparseVector(3, Array(0, 2), Array(0.1, 0.2))),
      Row(new SparseVector(3, Array(0), Array(0.1)), new SparseVector(3, Array(0, 2), Array(0.1, 0.2))),
      Row(new SparseVector(3, Array(0, 1), Array(0.1, 0.1)), new SparseVector(3, Array(0, 2), Array(0.1, 0.5)))
    )
    // e^[0.1, 0, 0.2] / e^[0.1, 0, 0.2 ] = [1.1, 1, 1.22] / [1.1, 1, 1.22] = [1, 1, 1]
    // e^[0.1, 0, 0] / e^[0.1, 0, 0.2 ] = [1.1, 1, 1] / [1.1, 1, 1.22] = [1, 1, 0.82]
    // e^[0.1, 0.1, 0] / e^[0.1, 0, 0.5 ] = [1.1, 1.1, 1] / [1.1, 1, 1.65] = [1, 1.1, 0.61]

    val fields = List(
      StructField("v1", VectorType, nullable = false),
      StructField("v2", VectorType, nullable = false))

    val data_rdd = spark.sparkContext.makeRDD(data)

    val df = spark.createDataFrame(data_rdd, StructType(fields))

    df.createOrReplaceTempView("my_table2")

    df.show()

    val add_function = new ExponentDivide().call _

    spark.udf.register("sparse_vector_exponent_divide", add_function)

    val out_df = spark.sql(
      "select sparse_vector_exponent_divide(v1, v2) as result from my_table2"
    )

    out_df.show()

    checkAnswer(
      out_df.selectExpr("result"),
      Seq(
        Row(new SparseVector(3, Array(0, 1, 2), Array(1.0, 1.0, 1.0))),
        Row(new SparseVector(3, Array(0, 1, 2), Array(1.0, 1.0, 0.8187307530779818))),
        Row(new SparseVector(3, Array(0, 1, 2), Array(1.0, 1.1051709180756477, 0.6065306597126334)))
      )
    )
    assert(3 == out_df.count())
  }
}
