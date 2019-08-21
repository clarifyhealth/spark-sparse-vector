package com.clarify.sparse_vectors
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.sql.api.java.UDF1
import scala.collection.mutable
import org.apache.spark.ml.linalg.Vectors
import scala.util.control.Breaks._

class DenseVectorToSparse extends UDF1[DenseVector, SparseVector] {

  override def call(v1: DenseVector): SparseVector = {
    dense_vector_to_sparse(v1)
  }

  def dense_vector_to_sparse(
      v1: DenseVector
  ): SparseVector = {
    return v1.toSparse
  }
}
