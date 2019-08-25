package com.clarify.sparse_vectors

import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql.api.java.UDF1

class SparseToDense extends UDF1[SparseVector, DenseVector] {

  override def call(v1: SparseVector): DenseVector = {
    sparse_vector_to_dense(v1)
  }

  def sparse_vector_to_dense(
      v1: SparseVector
  ): DenseVector = {
    v1.toDense
  }
}
