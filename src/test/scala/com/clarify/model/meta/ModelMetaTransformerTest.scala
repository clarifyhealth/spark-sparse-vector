package com.clarify.model.meta

import com.clarify.prediction.explainer.GLMExplainTransformer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.functions.{lit}

class ModelMetaTransformerTest extends QueryTest with SharedSparkSession {

  def initialize(): (DataFrame, DataFrame, DataFrame) = {
    val predictionDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(getClass.getResource("/basic/predictions.csv").getPath)

    val assembler = new VectorAssembler()
    assembler.setInputCols(predictionDF.columns.filter(_ != "ccg_id"))
    assembler.setOutputCol("feature_col")

    val predictionFinalDF = assembler.transform(predictionDF)

    predictionFinalDF.createOrReplaceTempView("my_predictions")

    val coefficientsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(getClass.getResource("/basic/coefficients.csv").getPath)
      .withColumn("model_id", lit("test_model_id"))

    coefficientsDF.createOrReplaceTempView("my_coefficients")

    val hccDescriptionDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(getClass.getResource("/basic/cms_hcc_descriptions.csv").getPath)

    hccDescriptionDF.createOrReplaceTempView("cms_hcc_descriptions")

    (predictionFinalDF, coefficientsDF, hccDescriptionDF)
  }

  test("test ModelMeta with Default") {

    spark.sharedState.cacheManager.clearCache()
    val nested = true

    val (_, _, _) = initialize()

    val explainTransformer = new GLMExplainTransformer()
    explainTransformer.setCoefficientView("my_coefficients")
    explainTransformer.setPredictionView("my_predictions")
    explainTransformer.setFamily("tweedie")
    explainTransformer.setLinkPower(0.5)
    explainTransformer.setVariancePower(1.0)
    explainTransformer.setNested(nested)
    explainTransformer.setCalculateSum(true)
    explainTransformer.setLabel("test")

    val df = spark.emptyDataFrame
    val explainDF = explainTransformer.transform(df)

    val metaTransformer = new ModelMetaTransformer()

    metaTransformer.setCoefficientView("my_coefficients")
    metaTransformer.setPredictionView("my_predictions")
    metaTransformer.setModelMetaView("my_model_meta")
    metaTransformer.setFamily("tweedie")
    metaTransformer.setLinkPower(0.5)
    metaTransformer.setLinkFunction("powerHalfLink")
    metaTransformer.setVariancePower(1.0)
    metaTransformer.setLabelCol("test")
    metaTransformer.setFeaturesCol("feature_col")

    val modelMetaDF = metaTransformer.transform(explainDF)

    assert(1 == modelMetaDF.count())

    val responseCol = List(
      "model_id",
      "pop_mean",
      "pop_contribution",
      "sigma_mean",
      "link_function ",
      "family",
      "link_power",
      "variance_power",
      "intercept",
      "coefficients",
      "features",
      "ohe_features",
      "feature_coefficients",
      "count_total",
      "mae",
      "rmse",
      "bias_avg",
      "r2",
      "zero_residuals",
      "f1",
      "accuracy",
      "aucPR",
      "aucROC",
      "weightedRecall",
      "weightedPrecision"
    )

    assert(responseCol exists (s => modelMetaDF.columns exists (_ contains s)))

    val responseDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(getClass.getResource("/basic/model_meta_default.csv").getPath)

    checkAnswer(
      modelMetaDF.select(
        "model_id",
        "sigma_mean",
        "link_function",
        "family",
        "link_power",
        "variance_power",
        "intercept",
        "feature_coefficients",
        "count_total",
        "mae",
        "rmse",
        "bias_avg",
        "r2",
        "zero_residuals",
        "f1",
        "accuracy",
        "aucPR",
        "aucROC",
        "weightedRecall",
        "weightedPrecision"
      ),
      responseDF
    )
  }

  test("test ModelMeta with Real Model Evaluation") {

    spark.sharedState.cacheManager.clearCache()
    val nested = true

    val (predictionDF, _, _) = initialize()

    predictionDF
      .selectExpr(
        "*",
        "cast(10 as double)  as count_total",
        "cast(0.2 as double) as mae",
        "cast(0.3 as double) as prediction_test_rmse",
        "cast(0.4 as double) as bias_avg",
        "cast(0.5 as double) as r2",
        "cast(0.6 as double) as zero_residuals",
        "cast(0.7 as double) as f1",
        "cast(0.8 as double) as accuracy",
        "cast(0.9 as double) as aucPR",
        "cast(0.1 as double) as aucROC",
        "cast(0.2 as double) as weightedRecall",
        "cast(0.3 as double) as weightedPrecision"
      )
      .createOrReplaceTempView("my_predictions")

    val explainTransformer = new GLMExplainTransformer()
    explainTransformer.setCoefficientView("my_coefficients")
    explainTransformer.setPredictionView("my_predictions")
    explainTransformer.setFamily("tweedie")
    explainTransformer.setLinkPower(0.5)
    explainTransformer.setVariancePower(1.0)
    explainTransformer.setNested(nested)
    explainTransformer.setCalculateSum(true)
    explainTransformer.setLabel("test")

    val df = spark.emptyDataFrame
    val explainDF = explainTransformer.transform(df)

    // explainDF.show()

    val metaTransformer = new ModelMetaTransformer()

    metaTransformer.setCoefficientView("my_coefficients")
    metaTransformer.setPredictionView("my_predictions")
    metaTransformer.setModelMetaView("my_model_meta")
    metaTransformer.setFamily("tweedie")
    metaTransformer.setLinkPower(0.5)
    metaTransformer.setLinkFunction("powerHalfLink")
    metaTransformer.setVariancePower(1.0)
    metaTransformer.setLabelCol("test")
    metaTransformer.setFeaturesCol("feature_col")

    val modelMetaDF = metaTransformer.transform(explainDF)

    modelMetaDF.show()

    assert(1 == modelMetaDF.count())

    val responseCol = List(
      "model_id",
      "pop_mean",
      "pop_contribution",
      "sigma_mean",
      "link_function ",
      "family",
      "link_power",
      "variance_power",
      "intercept",
      "coefficients",
      "features",
      "ohe_features",
      "feature_coefficients",
      "count_total",
      "mae",
      "rmse",
      "bias_avg",
      "r2",
      "zero_residuals",
      "f1",
      "accuracy",
      "aucPR",
      "aucROC",
      "weightedRecall",
      "weightedPrecision"
    )

    assert(responseCol exists (s => modelMetaDF.columns exists (_ contains s)))

    val responseDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(getClass.getResource("/basic/model_meta_eval_real.csv").getPath)

    checkAnswer(
      modelMetaDF.select(
        "model_id",
        "sigma_mean",
        "link_function",
        "family",
        "link_power",
        "variance_power",
        "intercept",
        "feature_coefficients",
        "count_total",
        "mae",
        "rmse",
        "bias_avg",
        "r2",
        "zero_residuals",
        "f1",
        "accuracy",
        "aucPR",
        "aucROC",
        "weightedRecall",
        "weightedPrecision"
      ),
      responseDF
    )
  }

}
