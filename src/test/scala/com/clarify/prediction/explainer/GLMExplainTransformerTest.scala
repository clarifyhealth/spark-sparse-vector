package com.clarify.prediction.explainer

import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{DataFrame, QueryTest}

class GLMExplainTransformerTest extends QueryTest with SharedSparkSession {

  def initialize(): (DataFrame, Map[String, Double]) = {
    val predictionDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(getClass.getResource("/basic/predictions.csv").getPath)

    val coefficientsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(getClass.getResource("/basic/coefficients.csv").getPath)

    coefficientsDF.createOrReplaceTempView("my_coefficients")

    val coefficients = coefficientsDF
      .select("Feature", "Coefficient")
      .filter("not Feature RLIKE '^.*_OHE___unknown$'")
      .collect()

    val allCoefficients = coefficients
      .map(row => (row.getAs[String](0) -> row.getAs[Double](1)))

    val featureCoefficients =
      allCoefficients.filter(x => x._1 != "Intercept").toMap

    (predictionDF, featureCoefficients)
  }

  test("test powerHalfLink") {

    spark.sharedState.cacheManager.clearCache()
    val nested = true

    val (predictionDF, featureCoefficients) = initialize()

    val explainTransformer = new GLMExplainTransformer()
    explainTransformer.setCoefficientView("my_coefficients")
    explainTransformer.setLinkFunctionType("powerHalfLink")
    explainTransformer.setNested(nested)
    explainTransformer.setCalculateSum(true)

    val resultDF = explainTransformer.transform(predictionDF)

    val contribPowerHalfLink = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(getClass.getResource("/basic/contribs_power_0.5_link.csv").getPath)
      .selectExpr(
        "ccg_id",
        "bround(contrib_intercept,3) as contrib_intercept",
        "bround(contrib_sum,3) as contrib_sum",
        "bround(calculated_prediction,3) as calculated_prediction"
      )
      .orderBy("ccg_id")

    checkAnswer(
      resultDF
        .selectExpr(
          "ccg_id",
          "bround(contrib_intercept,3) as contrib_intercept",
          "bround(contrib_sum,3) as contrib_sum",
          "bround(calculated_prediction,3) as calculated_prediction"
        )
        .orderBy("ccg_id"),
      contribPowerHalfLink
    )
  }

  test("test logLink") {

    spark.sharedState.cacheManager.clearCache()
    val nested = false

    val (predictionDF, featureCoefficients) = initialize()

    val explainTransformer = new GLMExplainTransformer()
    explainTransformer.setCoefficientView("my_coefficients")
    explainTransformer.setLinkFunctionType("logLink")
    explainTransformer.setNested(nested)
    explainTransformer.setCalculateSum(true)

    val resultDF = explainTransformer.transform(predictionDF)

    val logLinkDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(getClass.getResource("/basic/contribs_log_link.csv").getPath)
      .selectExpr(
        "ccg_id",
        "bround(contrib_intercept,3) as contrib_intercept",
        "bround(contrib_sum,3) as contrib_sum",
        "bround(calculated_prediction,3) as calculated_prediction"
      )
      .orderBy("ccg_id")

    checkAnswer(
      resultDF
        .selectExpr(
          "ccg_id",
          "bround(contrib_intercept,3) as contrib_intercept",
          "bround(contrib_sum,3) as contrib_sum",
          "bround(calculated_prediction,3) as calculated_prediction"
        )
        .orderBy("ccg_id"),
      logLinkDF
    )
  }

  test("test identityLink") {

    spark.sharedState.cacheManager.clearCache()
    val nested = true

    val (predictionDF, featureCoefficients) = initialize()

    val explainTransformer = new GLMExplainTransformer()
    explainTransformer.setCoefficientView("my_coefficients")
    explainTransformer.setLinkFunctionType("identityLink")
    explainTransformer.setNested(nested)
    explainTransformer.setCalculateSum(true)

    val resultDF = explainTransformer.transform(predictionDF)

    val identityLinkDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(getClass.getResource("/basic/contribs_identity_link.csv").getPath)
      .selectExpr(
        "ccg_id",
        "bround(contrib_intercept,3) as contrib_intercept",
        "bround(contrib_sum,3) as contrib_sum",
        "bround(calculated_prediction,3) as calculated_prediction"
      )
      .orderBy("ccg_id")

    checkAnswer(
      resultDF
        .selectExpr(
          "ccg_id",
          "bround(contrib_intercept,3) as contrib_intercept",
          "bround(contrib_sum,3) as contrib_sum",
          "bround(calculated_prediction,3) as calculated_prediction"
        )
        .orderBy("ccg_id"),
      identityLinkDF
    )

  }

  test("test logitLink") {

    spark.sharedState.cacheManager.clearCache()
    val nested = false

    val (predictionDF, featureCoefficients) = initialize()

    val explainTransformer = new GLMExplainTransformer()
    explainTransformer.setCoefficientView("my_coefficients")
    explainTransformer.setLinkFunctionType("logitLink")
    explainTransformer.setNested(nested)
    explainTransformer.setCalculateSum(true)

    val resultDF = explainTransformer.transform(predictionDF)

    val logitLinkDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(getClass.getResource("/basic/contribs_logit_link.csv").getPath)
      .selectExpr(
        "ccg_id",
        "bround(contrib_intercept,3) as contrib_intercept",
        "bround(contrib_sum,3) as contrib_sum",
        "bround(calculated_prediction,3) as calculated_prediction"
      )
      .orderBy("ccg_id")

    checkAnswer(
      resultDF
        .selectExpr(
          "ccg_id",
          "bround(contrib_intercept,3) as contrib_intercept",
          "bround(contrib_sum,3) as contrib_sum",
          "bround(calculated_prediction,3) as calculated_prediction"
        )
        .orderBy("ccg_id"),
      logitLinkDF
    )

  }

}
