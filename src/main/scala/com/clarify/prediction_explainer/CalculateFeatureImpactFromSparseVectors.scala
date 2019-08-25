package com.clarify.sparse_vectors

import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.api.java.UDF7
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class CalculateFeatureImpactFromSparseVectors
    extends UDF7[
      Double,
      Double,
      scala.collection.mutable.WrappedArray[java.lang.Object],
      SparseVector,
      SparseVector,
      SparseVector,
      SparseVector,
      Seq[(String, Double, Double, Double, Double)]
    ] {

  override def call(
                     outcome: Double,
                     pop_outcome: Double,
                     feature_list_native: scala.collection.mutable.WrappedArray[java.lang.Object],
                     pop_contribution: SparseVector,
                     ccg_level_contribution: SparseVector,
                     features: SparseVector,
                     feature_relative_contribution_exp_ohe: SparseVector
                   ): Seq[(String, Double, Double, Double, Double)] = {
    // spark can't serialize custom classes so we have to convert here
    val feature_list = feature_list_native.asInstanceOf[mutable.WrappedArray[GenericRowWithSchema]]
      .map(x => FeatureListItem(
        feature_index = x(0).asInstanceOf[Int],
        feature_name = x(1).asInstanceOf[String],
        base_feature_name = x(2).asInstanceOf[String]))
    get_feature_impact_from_sparse_vectors(outcome, pop_outcome,feature_list, pop_contribution, ccg_level_contribution,features, feature_relative_contribution_exp_ohe)
  }

  def get_feature_impact_from_sparse_vectors(
                                              outcome: Double,
                                              pop_outcome: Double,
                                              feature_list: Seq[FeatureListItem],
                                              pop_contribution: SparseVector,
                                              ccg_level_contribution: SparseVector,
                                              features: SparseVector,
                                              feature_relative_contribution_exp_ohe: SparseVector
                                            ): Seq[(String, Double, Double, Double, Double)] = {
    // Gets feature impact by choosing values from each vector with the same index

    var result = ListBuffer[(String, Double, Double, Double, Double)]()
    result += (("mean_prediction", pop_outcome, outcome, 0.0, 0.0))

    // first calculate contribution for features in v1
    for (i <- 0 until (feature_relative_contribution_exp_ohe.indices.size)) {
      result += ((
        Helpers.get_feature_name(feature_list, feature_relative_contribution_exp_ohe.indices(i)),
        Helpers.sparse_vector_get_float_by_index(pop_contribution, feature_relative_contribution_exp_ohe.indices(i), 1),
        Helpers.sparse_vector_get_float_by_index(ccg_level_contribution, feature_relative_contribution_exp_ohe.indices(i), 1),
        Helpers.sparse_vector_get_float_by_index(features, feature_relative_contribution_exp_ohe.indices(i), 0),
        feature_relative_contribution_exp_ohe.values(i)
      ))
    }

    result.toList
  }
}
