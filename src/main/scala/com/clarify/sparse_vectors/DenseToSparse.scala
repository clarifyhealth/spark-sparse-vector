package com.clarify.sparse_vectors
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql.api.java.UDF1

class DenseToSparse extends UDF1[DenseVector, SparseVector] {

  override def call(v1: DenseVector): SparseVector = {
    dense_vector_to_sparse(v1)
  }

  def dense_vector_to_sparse(
      v1: DenseVector
  ): SparseVector = {
    return v1.toSparse
  }
}
