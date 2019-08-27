package com.clarify.sparse_vectors

import org.apache.spark.ml.linalg.{DenseVector, Vector}
import org.apache.spark.sql.api.java.UDF1

class SparseToDense extends UDF1[Vector, DenseVector] {

  override def call(v1: Vector): DenseVector = {
    sparse_vector_to_dense(v1)
  }

  def sparse_vector_to_dense(
                              v1: Vector
  ): DenseVector = {
    v1 match {
      case vector: DenseVector =>
        vector
      case _ =>
        v1.toDense
    }
  }
}
