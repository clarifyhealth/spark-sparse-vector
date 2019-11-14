package com.clarify.prediction.explainer

import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{DataFrame, QueryTest}

class GLMExplainTransformerTest extends QueryTest with SharedSparkSession {

  def initialize(): (DataFrame, DataFrame) = {
    val predictionDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(getClass.getResource("/basic/predictions.csv").getPath)

    predictionDF.createOrReplaceTempView("my_predictions")

    val coefficientsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(getClass.getResource("/basic/coefficients.csv").getPath)

    coefficientsDF.createOrReplaceTempView("my_coefficients")

    (predictionDF, coefficientsDF)
  }

  test("test powerHalfLink") {

    spark.sharedState.cacheManager.clearCache()
    val nested = true

    val (predictionDF, coefficientsDF) = initialize()

    val explainTransformer = new GLMExplainTransformer()
    explainTransformer.setCoefficientView("my_coefficients")
    explainTransformer.setPredictionView("my_predictions")
    explainTransformer.setLinkFunctionType("powerHalfLink")
    explainTransformer.setNested(nested)
    explainTransformer.setCalculateSum(true)

    val df = spark.emptyDataFrame
    val resultDF = explainTransformer.transform(df)

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

    val (predictionDF, coefficientsDF) = initialize()

    val explainTransformer = new GLMExplainTransformer()
    explainTransformer.setCoefficientView("my_coefficients")
    explainTransformer.setPredictionView("my_predictions")
    explainTransformer.setLinkFunctionType("logLink")
    explainTransformer.setNested(nested)
    explainTransformer.setCalculateSum(true)

    val df = spark.emptyDataFrame
    val resultDF = explainTransformer.transform(df)

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

    val (predictionDF, coefficientsDF) = initialize()

    val explainTransformer = new GLMExplainTransformer()
    explainTransformer.setCoefficientView("my_coefficients")
    explainTransformer.setPredictionView("my_predictions")
    explainTransformer.setLinkFunctionType("identityLink")
    explainTransformer.setNested(nested)
    explainTransformer.setCalculateSum(true)

    val df = spark.emptyDataFrame
    val resultDF = explainTransformer.transform(df)

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

    val (predictionDF, coefficientsDF) = initialize()

    val explainTransformer = new GLMExplainTransformer()
    explainTransformer.setCoefficientView("my_coefficients")
    explainTransformer.setPredictionView("my_predictions")
    explainTransformer.setLinkFunctionType("logitLink")
    explainTransformer.setNested(nested)
    explainTransformer.setCalculateSum(true)

    val df = spark.emptyDataFrame
    val resultDF = explainTransformer.transform(df)

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

  test("test inverseLink") {

    spark.sharedState.cacheManager.clearCache()
    val nested = false

    val (predictionDF, coefficientsDF) = initialize()

    val explainTransformer = new GLMExplainTransformer()
    explainTransformer.setCoefficientView("my_coefficients")
    explainTransformer.setPredictionView("my_predictions")
    explainTransformer.setLinkFunctionType("inverseLink")
    explainTransformer.setNested(nested)
    explainTransformer.setCalculateSum(true)

    val df = spark.emptyDataFrame
    val resultDF = explainTransformer.transform(df)

    val logitLinkDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(getClass.getResource("/basic/contribs_inverse_link.csv").getPath)
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
