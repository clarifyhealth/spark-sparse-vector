package com.clarify.prediction_explainer

import com.clarify.sparse_vectors.{CalculateFeatureImpactFromSparseVectors, FeatureImpactItem, SparkSessionTestWrapper}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.QueryTest

class CalculateFeatureImpactFromSparseVectorsTest extends QueryTest with SparkSessionTestWrapper {

  val spark2 = spark

  import spark2.implicits._

  test("get feature impact for simple feature") {

    val row_log_odds_contribution_vector = new SparseVector(2, Array(0, 1), Array(0.1, 0.2))
    val population_log_odds_vector = new SparseVector(2, Array(0, 1), Array(0.1, 0.2))
    val features = new SparseVector(2, Array(0, 1), Array(0.1, 0.2))
    val feature_relative_contribution_exp_ohe = new SparseVector(2, Array(0, 1), Array(0.1, 0.2))
    val feature_list: Seq[String] = Seq("foo", "bar")
    val ohe_feature_list: Seq[String] = Seq("foo", "bar")
    val contribution = new CalculateFeatureImpactFromSparseVectors()
      .get_feature_impact_from_sparse_vectors(0.0, 0.0, feature_list, ohe_feature_list, population_log_odds_vector,
        row_log_odds_contribution_vector, features, feature_relative_contribution_exp_ohe)
    println(contribution)
    println(contribution.size)
    println("result class")
    println(contribution.getClass)
    val expected = Array(
      FeatureImpactItem("mean_prediction", 0.0, 0.0, 0.0, 0.0),
      FeatureImpactItem("foo", 0.1, 0.1, 0.1, 0.1),
      FeatureImpactItem("bar", 0.2, 0.2, 0.2, 0.2))
    println("expect class")
    println(expected.getClass)
    assert(contribution.toSeq == expected.toSeq)
  }
  test("calculate pop log odds data frame") {
    spark.sharedState.cacheManager.clearCache()
    val data = Seq(
      (
        0.0,
        0.0,
        Seq("foo", "bar", "zoo"),
        Seq("foo", "foo", "zoo"),
        new SparseVector(2, Array(0, 1), Array(0.1, 0.2)),
        new SparseVector(2, Array(0, 1), Array(0.1, 0.2)),
        new SparseVector(2, Array(0, 1), Array(0.1, 0.2)),
        new SparseVector(2, Array(0, 1), Array(0.1, 0.2))
      ),
      (
        0.0,
        0.0,
        Seq("foo", "bar", "zoo"),
        Seq("foo", "foo", "zoo"),
        new SparseVector(2, Array(0, 1), Array(0.1, 0.2)),
        new SparseVector(2, Array(0, 1), Array(0.1, 0.2)),
        new SparseVector(2, Array(0, 1), Array(0.1, 0.2)),
        new SparseVector(2, Array(0, 1), Array(0.1, 0.2))
      )
    ).toDF()
    val df = data.toDF(
      "outcome",
      "pop_outcome",
      "feature_list",
      "ohe_feature_list",
      "population_log_odds_vector",
      "row_log_odds_contribution_vector",
      "features",
      "feature_relative_contribution_exp_ohe")
    // df.withColumn("feature_list", df.col("feature_list").cast("array<struct<_1:int,_2:string,_3:string>>"))

    df.printSchema()
    df.createOrReplaceTempView("my_table2")

    df.show()

    val add_function = new CalculateFeatureImpactFromSparseVectors().call _

    spark.udf.register("get_feature_impact_from_sparse_vectors", add_function)

    val out_df = spark.sql(
      "select get_feature_impact_from_sparse_vectors(outcome, pop_outcome, feature_list, ohe_feature_list, population_log_odds_vector,row_log_odds_contribution_vector,features,feature_relative_contribution_exp_ohe) as result from my_table2"
    )

    out_df.show(truncate = false)

    out_df.printSchema()

    val expected = Seq(
      (
        Seq(
          ("mean_prediction", 0.0, 0.0, 0.0, 0.0),
          ("foo", 0.1, 0.1, 0.1, 0.1),
          ("bar", 0.2, 0.2, 0.2, 0.2)
        )
        ),
      Seq(
        ("mean_prediction", 0.0, 0.0, 0.0, 0.0),
        ("foo", 0.1, 0.1, 0.1, 0.1),
        ("bar", 0.2, 0.2, 0.2, 0.2))
    ).toDF(
      "result")

    checkAnswer(
      out_df.selectExpr("result"),
      expected
    )
    assert(2 == out_df.count())
  }
}
