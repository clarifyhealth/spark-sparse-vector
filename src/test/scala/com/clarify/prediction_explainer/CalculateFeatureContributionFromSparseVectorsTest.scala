package com.clarify.prediction_explainer

import com.clarify.sparse_vectors.SparkSessionTestWrapper
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.{QueryTest, SparkSession}

class CalculateFeatureContributionFromSparseVectorsTest extends QueryTest with SparkSessionTestWrapper {

  val spark2: SparkSession = spark

  import spark2.implicits._

  test("get feature impact for simple feature") {

    val row_log_odds_contribution_vector = new SparseVector(2, Array(0, 1), Array(0.1, 0.2))
    val population_log_odds_vector = new SparseVector(2, Array(0, 1), Array(0.1, 0.2))
    val features = new SparseVector(2, Array(0, 1), Array(0.1, 0.2))
    val feature_relative_contribution_exp_ohe = new SparseVector(2, Array(0, 1), Array(0.1, 0.2))
    val feature_list: Seq[String] = Seq("foo", "bar")
    val ohe_feature_list: Seq[String] = Seq("foo", "bar")
    val contribution = new CalculateFeatureContributionFromSparseVectors()
      .get_feature_contribution_from_sparse_vectors(0.0, 0.0,
        feature_list, ohe_feature_list,
        row_log_odds_contribution_vector, population_log_odds_vector,
        features, feature_relative_contribution_exp_ohe)
    println(contribution)
    println(contribution.length)
    println("result class")
    println(contribution.getClass)
    val expected = Array(
      FeatureContributionItem("mean_prediction", 0.0, 0.0, 0.0, 0.0),
      FeatureContributionItem("foo", 0.1, 0.1, 0.1, 0.1),
      FeatureContributionItem("bar", 0.2, 0.2, 0.2, 0.2))
    println("expect class")
    println(expected.getClass)
    assert(contribution.toSeq == expected.toSeq)
  }
  test("calculate pop log odds data frame") {
    spark.sharedState.cacheManager.clearCache()
    val data = Seq(
      (
        0.2, // row_outcome
        0.3, // pop_outcome
        Seq("male_ohe", "female_ohe", "age"), // feature_list
        Seq("gender", "gender", "age"), // ohe_feature_list
        new SparseVector(2, Array(0, 1), Array(0.1, 0.2)), // population_log_odds_contribution_vector
        new SparseVector(2, Array(0, 1), Array(0.1, 0.2)), // row_log_odds_contribution_vector
        new SparseVector(2, Array(0, 1), Array(0, 1)), // features
        new SparseVector(2, Array(0, 1), Array(0.1, 0.2)) // feature_relative_contribution_exp_ohe
      ),
      (
        0.4,
        0.3,
        Seq("male_ohe", "female_ohe", "age"), // feature_list
        Seq("gender", "gender", "age"), // ohe_feature_list
        new SparseVector(2, Array(0, 1), Array(0.1, 0.2)), // population_log_odds_contribution_vector
        new SparseVector(2, Array(0, 1), Array(0.1, 0.2)), // row_log_odds_contribution_vector
        new SparseVector(2, Array(0, 1), Array(1, 0)), // features
        new SparseVector(2, Array(0, 1), Array(0.1, 0.2)) // feature_relative_contribution_exp_ohe
      )
    ).toDF()
    val df = data.toDF(
      "row_outcome",
      "pop_outcome",
      "feature_list",
      "ohe_feature_list",
      "population_log_odds_contribution_vector",
      "row_log_odds_contribution_vector",
      "features",
      "feature_relative_contribution_exp_ohe")
    // df.withColumn("feature_list", df.col("feature_list").cast("array<struct<_1:int,_2:string,_3:string>>"))

    df.printSchema()
    df.createOrReplaceTempView("my_table2")

    df.show()

    val add_function = new CalculateFeatureContributionFromSparseVectors().call _

    spark.udf.register("get_feature_contribution_from_sparse_vectors", add_function)

    val out_df = spark.sql(
      """select get_feature_contribution_from_sparse_vectors(
        |row_outcome,
        |pop_outcome,
        |feature_list,
        |ohe_feature_list,
        |row_log_odds_contribution_vector,
        |population_log_odds_contribution_vector,
        |features,
        |feature_relative_contribution_exp_ohe
        |) as result from my_table2""".stripMargin
    )

    out_df.show(truncate = false)

    out_df.printSchema()

    val expected = Seq(
      Seq(
        ("mean_prediction", 0.3, 0.2, 0.0, 0.0),
        ("male_ohe", 0.1, 0.1, 0.0, 0.1),
        ("female_ohe", 0.2, 0.2, 1.0, 0.2)
      ),
      Seq(
        ("mean_prediction", 0.3, 0.4, 0.0, 0.0),
        ("male_ohe", 0.1, 0.1, 1.0, 0.1),
        ("female_ohe", 0.2, 0.2, 0.0, 0.2))
    ).toDF(
      "result")

    checkAnswer(
      out_df.selectExpr("result"),
      expected
    )
    assert(2 == out_df.count())
  }
}
