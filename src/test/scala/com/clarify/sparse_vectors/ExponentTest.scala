package com.clarify.sparse_vectors

import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{QueryTest, Row}

class ExponentTest extends QueryTest with SparkSessionTestWrapper {

  test("exponent simple") {
    val v1 = new SparseVector(3, Array(0), Array(0.1))
    val v3 = new Exponent().sparse_vector_exponent(v1)
    assert(v3 == new SparseVector(3, Array(0), Array(1.1051709180756477)))
  }
  test("exponent") {
    spark.sharedState.cacheManager.clearCache()

    val data = List(
      Row(new SparseVector(3, Array(0, 2), Array(0.1, 0.2))),
      Row(new SparseVector(3, Array(0, 2), Array(0.2, 0.1)))
    )

    val fields = List(
      StructField("v1", VectorType, nullable = false)
    )

    val data_rdd = spark.sparkContext.makeRDD(data)

    val df = spark.createDataFrame(data_rdd, StructType(fields))

    df.createOrReplaceTempView("my_table2")

    df.show()

    val add_function = new Exponent().call _

    spark.udf.register("sparse_vector_exponent", add_function)

    val out_df = spark.sql(
      "select sparse_vector_exponent(v1) as result from my_table2"
    )

    out_df.show()

    checkAnswer(
      out_df.selectExpr("result"),
      Seq(
        Row(new SparseVector(3, Array(0, 2), Array(1.1051709180756477, 1.2214027581601699))),
        Row(new SparseVector(3, Array(0, 2), Array(1.2214027581601699, 1.1051709180756477)))
      )
    )
    assert(2 == out_df.count())
  }
}
