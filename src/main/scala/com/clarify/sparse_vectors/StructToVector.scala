package com.clarify.sparse_vectors

import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

class StructToVector extends UDF1[Any, Vector] {

  override def call(datum: Any): Vector = {
    convert_struct_to_vector(datum)
  }

  def convert_struct_to_vector(
                                datum: Any
                              ): Vector = {
    datum match {
      case row: GenericRowWithSchema =>
        require(row.length == 4,
          s"VectorUDT.deserialize given row with length ${row.length} but requires length == 4")
        val tpe = row.getByte(0)
        tpe match {
          case 0 =>
            val size = row.getInt(1)
            val indices = row.getAs[Seq[Int]](2).toArray
            val values = row.getAs[Seq[Double]](3).toArray
            new SparseVector(size, indices, values)
          case 1 =>
            val values = row.getAs[Seq[Double]](3).toArray
            new DenseVector(values)
        }
    }
  }
}
