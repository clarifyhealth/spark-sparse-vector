package com.clarify.sparse_vectors

import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{QueryTest, Row}

class DenseToSparseTest extends QueryTest with SparkSessionTestWrapper {

  test("dense to sparse simple") {
    val v1 = new DenseVector(List(0.0, 1, 3, 0, 6).toArray)
    val v3 = new DenseToSparse().dense_vector_to_sparse(v1)
    assert(v3 == new SparseVector(5, Array(1, 2, 4), Array(1, 3, 6)))
  }
  test("dense to sparse with vectors") {
    spark.sharedState.cacheManager.clearCache()

    val data = List(
      Row(
        new DenseVector(List(0.0, 1, 3, 0, 6).toArray)
      ),
      Row(
        new DenseVector(List(1, 0.0, 3, 0, 6).toArray)
      ),
      Row(
        new DenseVector(List(0.0, 1, 3, 0, 0).toArray)
      )
    )

    // [0.1, 0, 0.2] - [0.1, 0, 0.2] = [0,0,0]
    // [0.1, 0, 0] - [0.1, 0 , 0.2] = [0, 0, -0.2]
    // [0.1, 0.1, 0] - [0.1, 0, 0.5] = [0, 0.1, -0.5]

    val fields = List(StructField("v1", VectorType, nullable = false))

    val data_rdd = spark.sparkContext.makeRDD(data)

    val df = spark.createDataFrame(data_rdd, StructType(fields))

    df.createOrReplaceTempView("my_table2")

    df.show()

    val add_function = new DenseToSparse().call _

    spark.udf.register("dense_vector_to_sparse", add_function)

    val out_df = spark.sql(
      "select dense_vector_to_sparse(v1) as result from my_table2"
    )

    out_df.show()

    checkAnswer(
      out_df.selectExpr("result"),
      Seq(
        Row(new SparseVector(5, Array(1, 2, 4), Array(1, 3, 6))),
        Row(new SparseVector(5, Array(0, 2, 4), Array(1, 3, 6))),
        Row(new SparseVector(5, Array(1, 2), Array(1, 3)))
      )
    )
    assert(3 == out_df.count())
  }
}
