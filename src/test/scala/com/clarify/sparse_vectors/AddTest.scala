package com.clarify.sparse_vectors

import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType

class AddTest extends QueryTest with SharedSparkSession {

  test("add") {
    spark.sharedState.cacheManager.clearCache()
    
    val data = List(Row(new SparseVector(3, Array(0), Array(0.1))),
      Row(new SparseVector(3, Array(0), Array(0.1))),
      Row(new SparseVector(3, Array(0), Array(0.1))))

    val fields = List(
      StructField("input_col", VectorType, nullable = false))

    val data_rdd = spark.sparkContext.makeRDD(data)

    val df = spark.createDataFrame(data_rdd, StructType(fields))

    df.createOrReplaceTempView("my_table2")

    df.show()

    val add_function = new SparseVectorAdd().call _

    spark.udf.register("sparse_vector_add", add_function)

    val out_df = spark.sql("select sparse_vector_add(input_col) from my_table2")

    // checkAnswer(out_df.selectExpr("count"), Seq(Row(1), Row(2)))

    out_df.show()

    // assert(6 == df.count())

  }

}

