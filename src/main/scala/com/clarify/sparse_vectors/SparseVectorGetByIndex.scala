package com.clarify.sparse_vectors
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import scala.collection.mutable
import org.apache.spark.ml.linalg.Vectors
import scala.util.control.Breaks._
import org.apache.spark.sql.api.java.UDF2
import org.apache.spark.sql.api.java.UDF3

class SparseVectorGetByIndex extends UDF3[SparseVector, Int, Double, Double] {

  override def call(
      v1: SparseVector,
      index: Int,
      default_value: Double
  ): Double = {
    Helpers.sparse_vector_get_float_by_index(v1, index, default_value)
  }

}
