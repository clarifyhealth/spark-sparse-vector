package com.clarify.sparse_vectors
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import scala.collection.mutable
import org.apache.spark.ml.linalg.Vectors
import scala.collection.immutable.TreeMap
import scala.util.control.Breaks._
import scala.util.control.Breaks._
import org.apache.spark.sql.api.java.UDF7

class CalculateFeatureImpactFromSparseVectors
    extends UDF7[
      Double,
      Double,
      Seq[(Int, String, String)],
      SparseVector,
      SparseVector,
      SparseVector,
      SparseVector,
      Seq[(String, Double, Double, Double)]
    ] {

  override def call(
    outcome: Double,
    pop_outcome: Double,
    feature_list: Seq[(Int, String, String)],
    pop_contribution: SparseVector,
    ccg_level_contribution: SparseVector,
    features: SparseVector,
    feature_relative_contribution_exp_ohe: SparseVector
  ): Seq[(String, Double, Double, Double)] = {
    get_feature_impact_from_sparse_vectors(outcome, pop_outcome,feature_list, pop_contribution, ccg_level_contribution,features, feature_relative_contribution_exp_ohe)
  }

  def get_feature_impact_from_sparse_vectors(
    outcome: Double,
    pop_outcome: Double,
    feature_list: Seq[(Int, String, String)],
    pop_contribution: SparseVector,
    ccg_level_contribution: SparseVector,
    features: SparseVector,
    feature_relative_contribution_exp_ohe: SparseVector
  ): Seq[(String, Double, Double, Double)] = {
    // Gets feature impact by choosing values from each vector with the same index

    var result = Seq[(String, Double, Double, Double)]()
    result :+ new Tuple5("mean_prediction", pop_outcome, outcome, 0.0, 0.0)

    // first calculate contribution for features in v1
    for (i <- 0 until (feature_relative_contribution_exp_ohe.indices.size)) {
      result :+ new Tuple5(
        Helpers.get_feature_name(feature_list, feature_relative_contribution_exp_ohe.indices(i)),
        Helpers.sparse_vector_get_float_by_index(pop_contribution, feature_relative_contribution_exp_ohe.indices(i), 1),
        Helpers.sparse_vector_get_float_by_index(ccg_level_contribution, feature_relative_contribution_exp_ohe.indices(i), 1),
        Helpers.sparse_vector_get_float_by_index(features, feature_relative_contribution_exp_ohe.indices(i), 0),
        feature_relative_contribution_exp_ohe.values(i)
      )
    }
    return result
  }
}
