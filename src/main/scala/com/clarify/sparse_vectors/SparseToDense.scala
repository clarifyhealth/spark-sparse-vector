package com.clarify.sparse_vectors
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.sql.api.java.UDF1
import scala.collection.mutable
import org.apache.spark.ml.linalg.Vectors
import scala.util.control.Breaks._

class SparseVectorToDense extends UDF1[SparseVector, DenseVector] {

  override def call(v1: SparseVector): DenseVector = {
    sparse_vector_to_dense(v1)
  }

  def sparse_vector_to_dense(
      v1: SparseVector
  ): DenseVector = {
    return v1.toDense
  }
}
