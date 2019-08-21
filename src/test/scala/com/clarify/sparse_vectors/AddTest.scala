package com.clarify.sparse_vectors

import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType

class AddTest extends QueryTest with SparkSessionTestWrapper {

  test("add simple") {
    val v1 = new SparseVector(3, Array(0), Array(0.1))
    val v2 = new SparseVector(3, Array(0), Array(0.1))
    val v3 = new SparseVectorAdd().sparse_vector_add(v1, v2)
    print(v3)
  }
  test("add to itself") {
    spark.sharedState.cacheManager.clearCache()

    val data = List(
      Row(new SparseVector(3, Array(0, 2), Array(0.1, 0.2))),
      Row(new SparseVector(3, Array(0), Array(0.1))),
      Row(new SparseVector(3, Array(0), Array(0.1)))
    )

    val fields = List(StructField("input_col", VectorType, nullable = false))

    val data_rdd = spark.sparkContext.makeRDD(data)

    val df = spark.createDataFrame(data_rdd, StructType(fields))

    df.createOrReplaceTempView("my_table2")

    df.show()

    val add_function = new SparseVectorAdd().call _

    spark.udf.register("sparse_vector_add", add_function)

    val out_df = spark.sql(
      "select sparse_vector_add(input_col, input_col) as result from my_table2"
    )

    out_df.show()
    
    checkAnswer(
      out_df.selectExpr("result"),
      Seq(
        Row(new SparseVector(3, Array(0, 2), Array(0.2, 0.4))),
        Row(new SparseVector(3, Array(0), Array(0.2))),
        Row(new SparseVector(3, Array(0), Array(0.2)))
      )
    )
    assert(3 == out_df.count())
  }
  test("add to other vectors") {
    spark.sharedState.cacheManager.clearCache()

    val data = List(
      Row(new SparseVector(3, Array(0, 2), Array(0.1, 0.2)), new SparseVector(3, Array(0, 2), Array(0.1, 0.2))),
      Row(new SparseVector(3, Array(0), Array(0.1)), new SparseVector(3, Array(0, 2), Array(0.1, 0.2))),
      Row(new SparseVector(3, Array(0,1), Array(0.1, 0.1)), new SparseVector(3, Array(0, 2), Array(0.1, 0.5)))
    )

    val fields = List(
      StructField("v1", VectorType, nullable = false),
      StructField("v2", VectorType, nullable = false))

    val data_rdd = spark.sparkContext.makeRDD(data)

    val df = spark.createDataFrame(data_rdd, StructType(fields))

    df.createOrReplaceTempView("my_table2")

    df.show()

    val add_function = new SparseVectorAdd().call _

    spark.udf.register("sparse_vector_add", add_function)

    val out_df = spark.sql(
      "select sparse_vector_add(v1, v2) as result from my_table2"
    )

    out_df.show()
    
    checkAnswer(
      out_df.selectExpr("result"),
      Seq(
        Row(new SparseVector(3, Array(0, 2), Array(0.2, 0.4))),
        Row(new SparseVector(3, Array(0, 2), Array(0.2, 0.2))),
        Row(new SparseVector(3, Array(0,1,2), Array(0.2, 0.1, 0.5)))
      )
    )
    assert(3 == out_df.count())
  }
}
