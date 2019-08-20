package com.clarify.sparse_vectors
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.sql.api.java.UDF1

class SparseVectorAdd extends UDF1[SparseVector, SparseVector] {

  override def call(v1: SparseVector): SparseVector = {

    Option(v1) match {
      case Some(v1) => v1
      case None => new SparseVector(3, Array(0), Array(0.1))
    }
  }

}