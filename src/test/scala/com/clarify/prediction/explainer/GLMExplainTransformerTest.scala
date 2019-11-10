package com.clarify.prediction.explainer

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, QueryTest, Row}

class GLMExplainTransformerTest extends QueryTest with SharedSparkSession {

  def calculateTotalContrib(
      df: DataFrame,
      featureCoefficients: Map[String, Double],
      prefixOrColumnName: String,
      nested: Boolean
  ): DataFrame = {
    val encoder =
      RowEncoder.apply(getSchema(df, List("contrib_sum")))
    df.map(
      mappingSumRows(df.schema, nested)(prefixOrColumnName, featureCoefficients)
    )(
      encoder
    )
  }

  private val mappingSumRows
      : (StructType, Boolean) => (String, Map[String, Double]) => Row => Row =
    (schema, nested) =>
      (prefixOrColumnName, featureCoefficients) =>
        (row) => {
          val calculate =
            if (nested)
              row
                .getMap[String, Double](schema.fieldIndex(prefixOrColumnName))
                .toMap
                .values
                .sum
            else
              featureCoefficients.map {
                case (featureName, _) =>
                  row
                    .getDouble(
                      schema.fieldIndex(s"${prefixOrColumnName}_${featureName}")
                    )
              }.sum
          val total = calculate + row.getDouble(
            schema.fieldIndex(s"contrib_intercept")
          )
          Row.merge(row, Row(total))
        }

  private def getSchema(
      df: DataFrame,
      columnNames: List[String]
  ): StructType = {
    var schema: StructType = df.schema
    columnNames.foreach {
      case (featureName) =>
        schema = schema.add(s"${featureName}", DataTypes.DoubleType, false)
    }
    schema
  }

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

    val resultDF = explainTransformer.transform(predictionDF)

    val contribDF =
      calculateTotalContrib(
        resultDF,
        featureCoefficients,
        "contrib",
        nested
      )

    val contribPowerHalfLink = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(getClass.getResource("/basic/contribs_power_0.5_link.csv").getPath)
      .selectExpr(
        "ccg_id",
        "bround(contrib_intercept,3) as contrib_intercept",
        "bround(contrib_sum,3) as contrib_sum",
        "bround(calculated_prediction,3)"
      )
      .orderBy("ccg_id")

    checkAnswer(
      contribDF
        .selectExpr(
          "ccg_id",
          "bround(contrib_intercept,3) as contrib_intercept",
          "bround(contrib_sum,3) as contrib_sum",
          "bround(calculated_prediction,3)"
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

    val resultDF = explainTransformer.transform(predictionDF)

    val contribDF =
      calculateTotalContrib(
        resultDF,
        featureCoefficients,
        "contrib",
        nested
      )

    val logLinkDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(getClass.getResource("/basic/contribs_log_link.csv").getPath)
      .selectExpr(
        "ccg_id",
        "bround(contrib_intercept,3) as contrib_intercept",
        "bround(contrib_sum,3) as contrib_sum",
        "bround(calculated_prediction,3)"
      )
      .orderBy("ccg_id")

    checkAnswer(
      contribDF
        .selectExpr(
          "ccg_id",
          "bround(contrib_intercept,3) as contrib_intercept",
          "bround(contrib_sum,3) as contrib_sum",
          "bround(calculated_prediction,3)"
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

    val resultDF = explainTransformer.transform(predictionDF)

    val contribDF =
      calculateTotalContrib(
        resultDF,
        featureCoefficients,
        "contrib",
        nested
      )

    val identityLinkDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(getClass.getResource("/basic/contribs_identity_link.csv").getPath)
      .selectExpr(
        "ccg_id",
        "bround(contrib_intercept,3) as contrib_intercept",
        "bround(contrib_sum,3) as contrib_sum",
        "bround(calculated_prediction,3)"
      )
      .orderBy("ccg_id")

    checkAnswer(
      contribDF
        .selectExpr(
          "ccg_id",
          "bround(contrib_intercept,3) as contrib_intercept",
          "bround(contrib_sum,3) as contrib_sum",
          "bround(calculated_prediction,3)"
        )
        .orderBy("ccg_id"),
      identityLinkDF
    )

  }

  ignore("test logitLink") {

    spark.sharedState.cacheManager.clearCache()
    val nested = false

    val (predictionDF, featureCoefficients) = initialize()

    val explainTransformer = new GLMExplainTransformer()
    explainTransformer.setCoefficientView("my_coefficients")
    explainTransformer.setLinkFunctionType("logitLink")
    explainTransformer.setNested(nested)

    val resultDF = explainTransformer.transform(predictionDF)

    val contribDF =
      calculateTotalContrib(
        resultDF,
        featureCoefficients,
        "contrib",
        nested
      )

    val logitLinkDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(getClass.getResource("/basic/contribs_logit_link.csv").getPath)
      .selectExpr(
        "ccg_id",
        "bround(contrib_intercept,3) as contrib_intercept",
        "bround(contrib_sum,3) as contrib_sum",
        "bround(calculated_prediction,3)"
      )
      .orderBy("ccg_id")

    checkAnswer(
      contribDF
        .selectExpr(
          "ccg_id",
          "bround(contrib_intercept,3) as contrib_intercept",
          "bround(contrib_sum,3) as contrib_sum",
          "bround(calculated_prediction,3)"
        )
        .orderBy("ccg_id"),
      logitLinkDF
    )

  }

}
