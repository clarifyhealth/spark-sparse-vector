package com.clarify.sparse_vectors

import org.apache.spark.ml.linalg.{SparseVector, Vector}
import org.apache.spark.sql.api.java.UDF1

class DenseToSparse extends UDF1[Vector, SparseVector] {

  override def call(v1: Vector): SparseVector = {
    dense_vector_to_sparse(v1)
  }

  def dense_vector_to_sparse(
                              v1: Vector
  ): SparseVector = {
    v1 match {
      case vector: SparseVector =>
        vector
      case _ =>
        v1.toSparse
    }
  }
}
