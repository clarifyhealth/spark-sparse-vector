package com.clarify.sparse_vectors
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import scala.collection.mutable
import org.apache.spark.ml.linalg.Vectors
import scala.collection.immutable.TreeMap
import scala.util.control.Breaks._
import scala.util.control.Breaks._
import org.apache.spark.sql.api.java.UDF3

class CalculatePopulationContribution
    extends UDF3[
      SparseVector,
      SparseVector,
      Seq[(Int, String, String)],
      SparseVector
    ] {

  override def call(
      v1: SparseVector,
      v2: SparseVector,
      feature_list: Seq[(Int, String, String)]
  ): SparseVector = {
    sparse_vector_calculate_population_contribution(v1, v2, feature_list)
  }

  def sparse_vector_calculate_population_contribution(
      v1: SparseVector,
      v2: SparseVector,
      feature_list: Seq[(Int, String, String)]
  ): SparseVector = {
    // Calculates the relative contribution of each element in a vector
    // if x1 and x2 are one hot encoded values of the same feature
    //     uses the formula e^B1x1/(e^B1X1*e^B2X2)
    // else
    //     uses the formula e^B1x1/(e^B1X1)
    // :param v1: current row's feature vector
    // :param v2: population feature vector
    // :param feature_list:

    val values: scala.collection.mutable.Map[Int, Double] =
      scala.collection.mutable.Map[Int, Double]()

    // first calculate contribution for features in v1
    for (i <- 0 until (v1.indices.size)) {
      // find the appropriate index on the other side
      val index = v1.indices(i)
      val feature_tuple = feature_list.filter(x => x._1 == index).head
      val base_feature_name = feature_tuple._2
      val related_feature_indices =
        feature_list.filter(x => x._3 == base_feature_name).map(x => x._1)

      var population_log: Double = 0
      for (j <- 0 until (v2.indices.size)) {
        if (related_feature_indices.contains(v2.indices(j))) {
          population_log = population_log + v2.values(j)
        }
      }
      values(v1.indices(i)) = population_log
    }
    // now add population contribution for features that are not in the ccg vector
    for (j <- 0 until (v2.indices.size)) {
      val index = v2.indices(j)
      if (v1.indices.contains(index) == false) {
        // find the feature's base feature name in feature_list
        val feature_tuple = feature_list.filter(x => x._1 == index).head
        val feature_name = feature_tuple._2
        val base_feature_name = feature_tuple._3
        if (feature_name == base_feature_name) { // not an OHE
          values(v2.indices(j)) = v2.values(j)
        }
      }
    }

    return Vectors.sparse(v1.size, Helpers.remove_zeros(values).toSeq).asInstanceOf[SparseVector]
  }
}
