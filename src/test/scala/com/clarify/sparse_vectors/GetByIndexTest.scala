package com.clarify.sparse_vectors

import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{QueryTest, Row}

class GetByIndexTest extends QueryTest with SparkSessionTestWrapper {

  test("get by index") {
    spark.sharedState.cacheManager.clearCache()

    val data = List(
      Row(
        new SparseVector(3, Array(0, 2), Array(0.1, 0.2)),
        new SparseVector(3, Array(0, 2), Array(0.1, 0.2))
      ),
      Row(
        new SparseVector(3, Array(0), Array(0.1)),
        new SparseVector(3, Array(0, 2), Array(0.1, 0.2))
      ),
      Row(
        new SparseVector(3, Array(0, 1), Array(0.1, 0.1)),
        new SparseVector(3, Array(0, 2), Array(0.1, 0.5))
      )
    )

    val fields = List(
      StructField("v1", VectorType, nullable = false),
      StructField("v2", VectorType, nullable = false))

    val data_rdd = spark.sparkContext.makeRDD(data)

    val df = spark.createDataFrame(data_rdd, StructType(fields))

    df.createOrReplaceTempView("my_table2")

    df.show()

    val add_function = new GetByIndex().call _

    spark.udf.register("sparse_vector_get_float_by_index", add_function)

    val out_df = spark.sql(
      "select sparse_vector_get_float_by_index(v1, 2, 0) as result from my_table2"
    )

    out_df.show()

    checkAnswer(
      out_df.selectExpr("result"),
      Seq(
        Row(
          0.2
        ),
        Row(
          0.0
        ),
        Row(
          0.0
        )
      )
    )
    assert(3 == out_df.count())
  }
}
