package com.clarify.prediction_explainer

import com.clarify.sparse_vectors.{CalculateFeatureImpactFromSparseVectors, CalculatePopulationContribution, FeatureListItem, SparkSessionTestWrapper}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.{QueryTest, Row}

class CalculateFeatureImpactFromSparseVectorsTest extends QueryTest with SparkSessionTestWrapper {

  val spark2 = spark

  import spark2.implicits._

  test("get feature impact for simple feature") {

    val row_log_odds_contribution_vector = new SparseVector(2, Array(0, 1), Array(0.1, 0.2))
    val population_log_odds_vector = new SparseVector(2, Array(0, 1), Array(0.1, 0.2))
    val features = new SparseVector(2, Array(0, 1), Array(0.1, 0.2))
    val feature_relative_contribution_exp_ohe = new SparseVector(2, Array(0, 1), Array(0.1, 0.2))
    val feature_list: Seq[FeatureListItem] = Seq(
      FeatureListItem(0, "foo", "foo"),
      FeatureListItem(1, "bar", "bar")
    )
    val contribution = new CalculateFeatureImpactFromSparseVectors()
      .get_feature_impact_from_sparse_vectors(0.0, 0.0, feature_list, population_log_odds_vector,
        row_log_odds_contribution_vector, features, feature_relative_contribution_exp_ohe)
    println(contribution)
    println(contribution.size)
    assert(contribution == List(
      ("mean_prediction", 0.0, 0.0, 0.0, 0.0),
      ("foo", 0.1, 0.1, 0.1, 0.1),
      ("bar", 0.2, 0.2, 0.2, 0.2))
    )
  }
  test("calculate pop log odds data frame") {
    spark.sharedState.cacheManager.clearCache()
    val data = Seq(
      (
        new SparseVector(3, Array(0, 1, 2), Array(0.2, 0.3, 0.4)),
        new SparseVector(3, Array(0, 1, 2), Array(0.3, 0.4, 0.5)),
        Seq((0, "foo", "foo"), (1, "bar", "foo"), (2, "zoo", "zoo"))
      ),
      (
        new SparseVector(3, Array(0, 2), Array(0.2, 0.4)),
        new SparseVector(3, Array(0, 1, 2), Array(0.3, 0.4, 0.5)),
        Seq((0, "foo", "foo"), (1, "bar", "foo"), (2, "zoo", "zoo"))
      )
    ).toDF()
    val df = data.toDF("v1", "v2", "feature_list")
    // df.withColumn("feature_list", df.col("feature_list").cast("array<struct<_1:int,_2:string,_3:string>>"))

    df.printSchema()
    df.createOrReplaceTempView("my_table2")

    df.show()

    val add_function = new CalculatePopulationContribution().call _

    spark.udf.register("sparse_vector_calculate_population_contribution_log_odds", add_function)

    val out_df = spark.sql(
      "select sparse_vector_calculate_population_contribution_log_odds(v1, v2, feature_list) as result from my_table2"
    )

    out_df.show()

    checkAnswer(
      out_df.selectExpr("result"),
      Seq(
        Row(new SparseVector(3, Array(0, 1, 2), Array(0.7, 0.7, 0.5))),
        Row(new SparseVector(3, Array(0, 2), Array(0.7, 0.5)))
      )
    )
    assert(2 == out_df.count())
  }
}
