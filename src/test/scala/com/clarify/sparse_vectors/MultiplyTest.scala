package com.clarify.sparse_vectors

import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{QueryTest, Row}

class MultiplyTest extends QueryTest with SparkSessionTestWrapper {

  test("multiply simple") {
    val v1 = new SparseVector(3, Array(0), Array(0.2))
    val v2 = new SparseVector(3, Array(0), Array(0.3))
    val v3 = new Multiply().sparse_vector_multiply(v1, v2)
    assert(v3 == new SparseVector(3, Array(0), Array(0.06)))
  }
  test("multiply") {
    spark.sharedState.cacheManager.clearCache()

    val data = List(
      Row(new SparseVector(3, Array(0, 2), Array(0.1, 0.2)), new SparseVector(3, Array(0, 2), Array(0.1, 0.2))),
      Row(new SparseVector(3, Array(0), Array(0.1)), new SparseVector(3, Array(0, 2), Array(0.1, 0.2))),
      Row(new SparseVector(3, Array(0, 1), Array(0.1, 0.1)), new SparseVector(3, Array(0, 2), Array(0.1, 0.5)))
    )

    // [0.1, 0, 0.2 ] x [0.1, 0, 0.2 ] = [0.1, 0, 0.4 ]
    // [0.1, 0, 0 ] x [0., 0, 0.2 ] = [0.1, 0, 0 ]
    // [0.1, 0.1, 0 ] x [0.1, 0, 0.5 ] = [0.1, 0, 0 ]

    val fields = List(
      StructField("v1", VectorType, nullable = false),
      StructField("v2", VectorType, nullable = false))

    val data_rdd = spark.sparkContext.makeRDD(data)

    val df = spark.createDataFrame(data_rdd, StructType(fields))

    df.createOrReplaceTempView("my_table2")

    df.show()

    val add_function = new Multiply().call _

    spark.udf.register("sparse_vector_multiply", add_function)

    val out_df = spark.sql(
      "select sparse_vector_multiply(v1, v2) as result from my_table2"
    )

    out_df.show()

    checkAnswer(
      out_df.selectExpr("result"),
      Seq(
        Row(new SparseVector(3, Array(0, 2), Array(0.010000000000000002, 0.04000000000000001))),
        Row(new SparseVector(3, Array(0), Array(0.010000000000000002))),
        Row(new SparseVector(3, Array(0), Array(0.010000000000000002)))
      )
    )
    assert(3 == out_df.count())
  }
}
