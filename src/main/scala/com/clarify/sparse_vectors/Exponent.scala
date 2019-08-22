package com.clarify.sparse_vectors

import org.apache.spark.ml.linalg.{SparseVector, Vectors}
import org.apache.spark.sql.api.java.UDF1

class Exponent extends UDF1[SparseVector, SparseVector] {

  override def call(v1: SparseVector): SparseVector = {
    sparse_vector_exponent(v1)
  }

  def sparse_vector_exponent(
      v1: SparseVector
  ): SparseVector = {
    val values: scala.collection.mutable.Map[Int, Double] =
      scala.collection.mutable.Map[Int, Double]()
    // Add values from v1
    for (i <- 0 until v1.indices.length) {
      val index = v1.indices(i)
      values(index) = Math.exp(v1.values(i))
    }
    Vectors.sparse(v1.size, Helpers.remove_zeros(values).toSeq).asInstanceOf[SparseVector]
  }
}
