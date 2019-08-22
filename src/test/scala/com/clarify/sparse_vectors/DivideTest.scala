package com.clarify.sparse_vectors

import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{QueryTest, Row}

class DivideTest extends QueryTest with SparkSessionTestWrapper {

  test("divide simple") {
    val v1 = new SparseVector(3, Array(0), Array(6))
    val v2 = new SparseVector(3, Array(0), Array(2))
    val v3 = new Divide().sparse_vector_divide(v1, v2)
    assert(v3 === new SparseVector(3, Array(0), Array(3)))
  }
  test("divide by other vectors") {
    spark.sharedState.cacheManager.clearCache()

    val data = List(
      Row(
        new SparseVector(3, Array(0, 2), Array(6, 16)), new SparseVector(3, Array(0, 2), Array(3, 2))
      ),
      Row(
        new SparseVector(3, Array(0), Array(8)), new SparseVector(3, Array(0, 2), Array(2, 3))
      ),
      Row(
        new SparseVector(3, Array(0, 1), Array(6, 16)), new SparseVector(3, Array(0), Array(2))
        // [6,16,0] / [2, 0, 0] = [3, Infinity ]
      )
    )

    val fields = List(
      StructField("v1", VectorType, nullable = false),
      StructField("v2", VectorType, nullable = false))

    val data_rdd = spark.sparkContext.makeRDD(data)

    val df = spark.createDataFrame(data_rdd, StructType(fields))

    df.createOrReplaceTempView("my_table2")

    df.show(truncate = false)

    val add_function = new Divide().call _

    spark.udf.register("sparse_vector_divide", add_function)

    val out_df = spark.sql(
      "select sparse_vector_divide(v1, v2) as result from my_table2"
    )

    out_df.show(truncate = false)

    checkAnswer(
      out_df.selectExpr("result"),
      Seq(
        Row(new SparseVector(3, Array(0, 2), Array(2, 8))),
        Row(new SparseVector(3, Array(0), Array(4))),
        Row(new SparseVector(3, Array(0, 1), Array(3, Double.PositiveInfinity)))
      )
    )
    assert(3 == out_df.count())
  }
}
