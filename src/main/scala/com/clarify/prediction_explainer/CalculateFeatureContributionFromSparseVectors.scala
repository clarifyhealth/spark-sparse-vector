package com.clarify.prediction_explainer

import com.clarify.sparse_vectors.Helpers
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.api.java.UDF8

import scala.collection.mutable.ListBuffer

case class FeatureContributionItem(
                                    feature_name: String,
                                    pop_contribution: Float,
                                    row_contribution: Float,
                                    value: Float,
                                    relative_contribution: Float
                            ) extends Serializable

class CalculateFeatureContributionFromSparseVectors
  extends UDF8[
    Double,
    Double,
    Seq[String],
    Seq[String],
    SparseVector,
    SparseVector,
    SparseVector,
    SparseVector,
    Array[FeatureContributionItem]
  ] {

  override def call(
                     row_outcome: Double,
                     pop_outcome: Double,
                     feature_list: Seq[String],
                     ohe_feature_list: Seq[String],
                     pop_contribution: SparseVector,
                     row_level_contribution: SparseVector,
                     features: SparseVector,
                     feature_relative_contribution_exp_ohe: SparseVector
                   ): Array[FeatureContributionItem] = {
    get_feature_contribution_from_sparse_vectors(row_outcome, pop_outcome,
      feature_list, ohe_feature_list,
      row_level_contribution, pop_contribution,
      features, feature_relative_contribution_exp_ohe)
  }

  def get_feature_contribution_from_sparse_vectors(
                                                    row_outcome: Double, pop_outcome: Double,
                                                    feature_list: Seq[String], ohe_feature_list: Seq[String],
                                                    row_level_contribution: SparseVector, pop_contribution: SparseVector,
                                                    features: SparseVector,
                                                    feature_relative_contribution_exp_ohe: SparseVector): Array[FeatureContributionItem] = {
    // Gets feature impact by choosing values from each vector with the same index

    var result = ListBuffer[FeatureContributionItem]()
    result += FeatureContributionItem("mean_prediction", pop_outcome.toFloat, row_outcome.toFloat, 0.0f, 0.0f)

    // first calculate contribution for features in v1
    for (i <- feature_relative_contribution_exp_ohe.indices.indices) {
      result += FeatureContributionItem(
        feature_list(feature_relative_contribution_exp_ohe.indices(i)),
        Helpers.sparse_vector_get_float_by_index(pop_contribution, feature_relative_contribution_exp_ohe.indices(i), 1),
        Helpers.sparse_vector_get_float_by_index(row_level_contribution, feature_relative_contribution_exp_ohe.indices(i), 1),
        Helpers.sparse_vector_get_float_by_index(features, feature_relative_contribution_exp_ohe.indices(i), 0),
        feature_relative_contribution_exp_ohe.values(i).toFloat
      )
    }

    result.toArray
  }
}
