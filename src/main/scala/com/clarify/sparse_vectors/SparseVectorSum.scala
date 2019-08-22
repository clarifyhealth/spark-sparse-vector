package com.clarify.sparse_vectors
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import scala.collection.mutable
import org.apache.spark.ml.linalg.Vectors
import scala.util.control.Breaks._
import org.apache.spark.sql.api.java.UDF2

class SparseVectorSum extends UDF2[SparseVector, Double, Double] {

  override def call(v1: SparseVector, initial_value: Double): Double = {
    sparse_vector_sum(v1, initial_value)
  }

  def sparse_vector_sum(
      v1: SparseVector,
      initial_value: Double
  ): Double = {
      var sum: Double = initial_value
    for (i <- 0 until (v1.indices.size)) {
      val index = v1.indices(i)
      sum = sum + v1.values(i)
    }
    return sum
  }
}
