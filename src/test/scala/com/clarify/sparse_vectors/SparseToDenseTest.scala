package com.clarify.sparse_vectors

import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType

class SparseToDenseTest extends QueryTest with SparkSessionTestWrapper {

  test("sparse to dense simple") {
    val v1 = new SparseVector(5, Array(1, 2, 4), Array(1, 3, 6))
    val v3 = new SparseVectorToDense().sparse_vector_to_dense(v1)
    assert(v3 == new DenseVector(List(0.0, 1, 3, 0, 6).toArray))
  }
  test("sparse to dense with vectors") {
    spark.sharedState.cacheManager.clearCache()

    val data = List(
      Row(
        new SparseVector(5, Array(1, 2, 4), Array(1, 3, 6))
      ),
      Row(
        new SparseVector(5, Array(0, 2, 4), Array(1, 3, 6))
      ),
      Row(
        new SparseVector(5, Array(1, 2), Array(1, 3))
      )
    )

    val fields = List(StructField("v1", VectorType, nullable = false))

    val data_rdd = spark.sparkContext.makeRDD(data)

    val df = spark.createDataFrame(data_rdd, StructType(fields))

    df.createOrReplaceTempView("my_table2")

    df.show()

    val add_function = new SparseVectorToDense().call _

    spark.udf.register("sparse_vector_to_dense", add_function)

    val out_df = spark.sql(
      "select sparse_vector_to_dense(v1) as result from my_table2"
    )

    out_df.show()

    checkAnswer(
      out_df.selectExpr("result"),
      Seq(
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
    )

    assert(3 == out_df.count())
  }
}
