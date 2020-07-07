package com.clarify.model.meta

import org.apache.log4j.Logger
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.{Param, ParamMap, StringArrayParam}
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.ml.util.{
  DefaultParamsReadable,
  DefaultParamsWritable,
  Identifiable
}
import org.apache.spark.sql.functions.{avg, lit, substring_index}
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.json4s._
import org.json4s.jackson.Json

import scala.collection.SortedMap

class ModelMetaTransformer(override val uid: String)
    extends Transformer
    with DefaultParamsWritable {

  val logger: Logger = Logger.getLogger(getClass)

  def this() = this(Identifiable.randomUID("ModelMetaTransformer"))

  // Transformer Params
  // Defining a Param requires 3 elements:
  //  - Param definition
  //  - Param getter method
  //  - Param setter method
  // (The getter and setter are technically not required, but they are nice standards to follow.)

  /**
    * Param for predictionView view name.
    */
  final val predictionView: Param[String] =
    new Param[String](
      this,
      "predictionView",
      "input predictionView view name"
    )

  final def getPredictionView: String = $(predictionView)

  final def setPredictionView(value: String): ModelMetaTransformer =
    set(predictionView, value)

  /**
    * Param for coefficientView name.
    */
  final val coefficientView: Param[String] =
    new Param[String](this, "coefficientView", "input coefficient view name")

  final def getCoefficientView: String = $(coefficientView)

  final def setCoefficientView(value: String): ModelMetaTransformer =
    set(coefficientView, value)

  /**
    * Param for modelMetaView name.
    */
  final val modelMetaView: Param[String] =
    new Param[String](this, "modelMetaView", "input model meta view name")

  final def getModelMetaView: String = $(modelMetaView)

  final def setModelMetaView(value: String): ModelMetaTransformer =
    set(modelMetaView, value)

  /**
    * Param for link function type .
    */
  final val linkFunction: Param[String] =
    new Param[String](this, "linkFunction", "input linkFunction")

  final def getLinkFunction: String = $(linkFunction)

  final def setLinkFunction(value: String): ModelMetaTransformer =
    set(linkFunction, value)

  /**
    * Param for label name.
    */
  final val labelCol: Param[String] =
    new Param[String](
      this,
      "labelCol",
      "training label name"
    )

  final def getLabelCol: String = $(labelCol)

  final def setLabelCol(value: String): ModelMetaTransformer =
    set(labelCol, value)

  /**
    * Param for features name.
    */
  final val featuresCol: Param[String] =
    new Param[String](
      this,
      "featuresCol",
      "training features col name"
    )

  final def getFeaturesCol: String = $(featuresCol)

  final def setFeaturesCol(value: String): ModelMetaTransformer =
    set(featuresCol, value)

  /**
    * Param for family name.
    */
  final val family: Param[String] =
    new Param[String](
      this,
      "family",
      "glm family name"
    )

  final def getFamily: String = $(family)

  final def setFamily(value: String): ModelMetaTransformer =
    set(family, value)

  /**
    * Param for variancePower.
    */
  final val variancePower: Param[Double] =
    new Param[Double](
      this,
      "variancePower",
      "tweedie variancePower"
    )

  final def getVariancePower: Double = $(variancePower)

  final def setVariancePower(value: Double): ModelMetaTransformer =
    set(variancePower, value)

  /**
    * Param for linkPower.
    */
  final val linkPower: Param[Double] =
    new Param[Double](
      this,
      "linkPower",
      "tweedie linkPower"
    )

  final def getLinkPower: Double = $(linkPower)

  final def setLinkPower(value: Double): ModelMetaTransformer =
    set(linkPower, value)

  /**
    * Param for model category.
    */
  final val modelCategory: Param[String] =
    new Param[String](
      this,
      "modelCategory",
      "modelCategory : predictive vs diagnostic"
    )

  final def getModelCategory: String = $(modelCategory)

  final def setModelCategory(value: String): ModelMetaTransformer =
    set(modelCategory, value)

  /**
    * Param for model category.
    */
  final val modelMetasPaths: StringArrayParam =
    new StringArrayParam(
      this,
      "modelMetasPaths",
      "modelMetasPaths : The list of diagnostic modelMetas"
    )

  final def getModelMetasPaths: Array[String] = $(modelMetasPaths)

  final def setModelMetasPaths(value: Array[String]): ModelMetaTransformer =
    set(modelMetasPaths, value)

  // (Optional) You can set defaults for Param values if you like.
  setDefault(
    predictionView -> "predictions",
    coefficientView -> "coefficient",
    modelMetaView -> "modelMeta",
    linkFunction -> "other",
    labelCol -> "test",
    featuresCol -> "features",
    family -> "gaussian",
    linkPower -> 0.0,
    variancePower -> -1.0,
    modelCategory -> "diagnostic",
    modelMetasPaths -> Array()
  )

  // Transformer requires 3 methods:
  //  - transform
  //  - transformSchema
  //  - copy

  /**
    * This method implements the main transformation.
    * Its required semantics are fully defined by the method API: take a Dataset or DataFrame,
    * and return a DataFrame.
    *
    * Most Transformers are 1-to-1 row mappings which add one or more new columns and do not
    * remove any columns.  However, this restriction is not required.  This example does a flatMap,
    * so we could either (a) drop other columns or (b) keep other columns, making copies of values
    * in each row as it expands to multiple rows in the flatMap.  We do (a) for simplicity.
    */
  override def transform(dataset: Dataset[_]): DataFrame = {
    import dataset.sqlContext.implicits._

    logger.info(
      s"""All parameters 
         | predictionView=${getPredictionView} ,
         | coefficientView=${getCoefficientView} ,
         | modelMetaView=${getModelMetaView} , 
         | linkFunction=${getLinkFunction} , 
         | labelCol=${getLabelCol} ,
         | featuresCol=${getFeaturesCol} ,
         | family=${getFamily} ,
         | linkPower=${getLinkPower} ,
         | variancePower= ${getVariancePower}""".stripMargin
    )

    logger.info(s"Loading CoefficientView ${getCoefficientView}")
    // Load coefficientView
    val coefficientsDF = dataset.sqlContext
      .table(getCoefficientView)

    logger.info(s"Loading PredictionView ${getPredictionView}")
    // Load predictionView
    val predictionsDF = dataset.sqlContext.table(getPredictionView)
    val predictionsSampleDF = getRandomNSample(predictionsDF)

    logger.info(s"Loading cms_hcc_descriptions")
    // Load cms_hcc_descriptions
    val hccDescriptionsDF = dataset.sqlContext.table("cms_hcc_descriptions")

    val coefficients = coefficientsDF
      .select("Feature_Index", "Feature", "Coefficient")
      .withColumn("ohe_features", substring_index($"Feature", "_OHE", 1))
      .orderBy("Feature_Index")
      .collect()

    logger.info(
      s"Start ${getPredictionView} population feature and contrib means calculation"
    )
    // The most expensive operation
    val sigma_mean_col = s"prediction_${getLabelCol}_sigma"
    val population_means = predictionsSampleDF
      .select(
        Summarizer.mean($"${getFeaturesCol}").alias("pop_mean"),
        Summarizer.mean($"contrib_vector").alias("pop_contribution"),
        if (predictionsSampleDF.columns.contains(sigma_mean_col))
          avg(sigma_mean_col).alias("sigma_mean")
        else lit(0.0).cast(DoubleType).alias("sigma_mean")
      )
      .collect()(0)
    logger.info(
      s"Done ${getPredictionView} population feature and contrib means calculation"
    )

    // Calculate the population feature and contrib means
    val pop_mean = population_means.getAs[Vector]("pop_mean").toArray
    val pop_contribution =
      population_means.getAs[Vector]("pop_contribution").toArray
    val sigma_mean = population_means.getAs[Double]("sigma_mean")

    val featureIndexMap = SortedMap(
      coefficients
        .map(
          row =>
            row.getAs[Long](0) -> (row.getAs[String](1), row
              .getAs[Double](2), row.getAs[String](3))
        ): _*
    )

    // Intercept
    val intercept =
      featureIndexMap.getOrElse(-1, ("Intercept", 0.0, "Intercept"))._2

    // Feature and Coefficient
    val featureCoefficients =
      featureIndexMap.filter(x => x._2._1 != "Intercept")

    // Original Feature Name
    val featureArray = featureCoefficients.map {
      case (_, (featureName, _, _)) => featureName
    }.toArray

    // Original Feature Coefficient
    val coefficientArray = featureCoefficients.map {
      case (_, (_, coefficient, _)) => coefficient
    }.toArray

    // OHE Feature
    val oheFeatureArray = featureCoefficients.map {
      case (_, (_, _, oheFeature)) => oheFeature
    }.toArray

    // hcc descriptions handling
    val hccDescriptions = hccDescriptionsDF
      .collect()
      .map(
        row =>
          (row.getAs[String]("hcc_code") -> row.getAs[String]("description"))
      )
      .toMap

    val hccDescriptionsMap = featureCoefficients.map {
      case (_, (featureName, coefficient, _)) =>
        s"${featureName} (${hccDescriptions.getOrElse(featureName, "")})" -> coefficient
    }

    val hccDescriptionsJson = Json(DefaultFormats).write(hccDescriptionsMap)

    logger.info(
      s"Done ${getPredictionView} HCC Description mapping ${hccDescriptionsJson}"
    )

    logger.info(
      s"Start ${getPredictionView}  Coefficient Summary"
    )
    val summaryRow = getModelCategory match {
      case "predictive" =>
        coefficientsDF
          .selectExpr("concat_ws('-',model_id,'X') as model_id")
          .limit(1)
      case _ => coefficientsDF.select($"model_id").limit(1)
    }

    val summaryRowCoefficientsDF = summaryRow
      .withColumn("pop_mean", lit(pop_mean))
      .withColumn("pop_contribution", lit(pop_contribution))
      .withColumn("sigma_mean", lit(sigma_mean).cast(DoubleType))
      .withColumn("link_function", lit(getLinkFunction))
      .withColumn("family", lit(getFamily))
      .withColumn("link_power", lit(getLinkPower).cast(DoubleType))
      .withColumn("variance_power", lit(getLinkPower).cast(DoubleType))
      .withColumn("intercept", lit(intercept).cast(DoubleType))
      .withColumn("coefficients", lit(coefficientArray))
      .withColumn("features", lit(featureArray))
      .withColumn("ohe_features", lit(oheFeatureArray))
      .withColumn("feature_coefficients", lit(hccDescriptionsJson))
      .withColumn("model_category", lit("diagnostic"))

    logger.info(
      s"Done ${getPredictionView} Coefficient Summary"
    )

    logger.info(
      s"Start ${getPredictionView} Prediction Summary"
    )
    val predictionsOneRowDF = predictionsSampleDF.limit(1)

    val projections =
      buildAppendMetricExpression(predictionsOneRowDF, getLabelCol)

    val firstDF = summaryRowCoefficientsDF.selectExpr(projections: _*)

    val finalDF = getModelCategory match {
      case "predictive" => {

        val diagnosticModelMetaDF =
          dataset.sqlContext.read.load(getModelMetasPaths: _*).limit(1)
        val rowSchema = diagnosticModelMetaDF.schema
        val diagnosticRow = diagnosticModelMetaDF.collect()(0)

        val hccDescriptionsJsonConcat = concatFeatureDesc(
          diagnosticRow
            .getString(rowSchema.fieldIndex("feature_coefficients")),
          hccDescriptionsJson
        )
        val secondSummaryRow = coefficientsDF.select($"model_id").limit(1)
        val secondSummaryRowDF = secondSummaryRow
          .withColumn(
            "pop_mean",
            lit(
              diagnosticRow
                .getSeq[Double](rowSchema.fieldIndex("pop_mean"))
                .toArray ++ pop_mean
            )
          )
          .withColumn(
            "pop_contribution",
            lit(
              diagnosticRow
                .getSeq[Double](rowSchema.fieldIndex("pop_contribution"))
                .toArray ++ pop_contribution
            )
          )
          .withColumn("sigma_mean", lit(-1.0).cast(DoubleType))
          .withColumn("link_function", lit("na"))
          .withColumn("family", lit("na"))
          .withColumn("link_power", lit(-1.0).cast(DoubleType))
          .withColumn("variance_power", lit(-1.0).cast(DoubleType))
          .withColumn(
            "intercept",
            lit(
              diagnosticRow
                .getDouble(rowSchema.fieldIndex("intercept")) + intercept
            ).cast(DoubleType)
          )
          .withColumn(
            "coefficients",
            lit(
              diagnosticRow
                .getSeq[Double](rowSchema.fieldIndex("coefficients"))
                .toArray ++ coefficientArray
            )
          )
          .withColumn(
            "features",
            lit(
              diagnosticRow
                .getSeq[String](rowSchema.fieldIndex("features"))
                .toArray ++ featureArray
            )
          )
          .withColumn(
            "ohe_features",
            lit(
              diagnosticRow
                .getSeq[String](rowSchema.fieldIndex("ohe_features"))
                .toArray ++ oheFeatureArray
            )
          )
          .withColumn("feature_coefficients", lit(hccDescriptionsJsonConcat))
          .withColumn("model_category", lit("predictive"))

        val projections =
          buildAppendMetricExpression(
            predictionsOneRowDF,
            if (getLabelCol.startsWith("residual"))
              getLabelCol.replace("residual", "predictive")
            else
              getLabelCol.replace("ncm", "predictive")
          )
        val secondDF = secondSummaryRowDF.selectExpr(projections: _*)
        firstDF.union(secondDF)
      }
      case _ => firstDF
    }

    // finalDF.show(truncate = false)

    finalDF.createOrReplaceTempView(getModelMetaView)

    logger.info(
      s"Done ${getPredictionView} Prediction Summary"
    )

    finalDF
  }

  def concatFeatureDesc(first: String, second: String): String = {
    s"""{${first.replace("{", "").replace("}", "")},
         ${second.replace("{", "").replace("}", "")}}"""
  }

  /**
    *
    * @param prediction
    * @param label
    * @return
    */
  def buildAppendMetricExpression(
      prediction: DataFrame,
      label: String
  ): Seq[String] = {
    val regressionMetric =
      fetchRegressionMetric(prediction, label)
    val customMetric = fetchCustomMetric(prediction, label)
    val classificationMetric =
      fetchClassificationMetric(prediction, label)

    val projections = Seq("*") ++ regressionMetric.map {
      case (key, value) => s"cast(${value} as double) as ${key}"
    } ++ customMetric.map {
      case (key, value) => s"cast(${value} as double) as ${key}"
    } ++ classificationMetric.map {
      case (key, value) => s"cast(${value} as double) as ${key}"
    }
    projections
  }

  /**
    * Extract the regression metric from a single row prediction DataFrame
    * @param prediction This is one Row DataFrame
    * @param label
    * @return
    */
  def fetchRegressionMetric(
      prediction: DataFrame,
      label: String
  ): Map[String, AnyVal] = {
    if (prediction.columns.contains("regression_metrics")) {
      val tempRow =
        prediction
          .selectExpr(
            "size(regression_metrics) as regression_count"
          )
          .collect()(0)
      val predictionLabel = s"prediction_${label}"
      if (tempRow.getInt(0) > 0) {
        val oneRow = prediction
          .selectExpr(
            s"regression_metrics.${predictionLabel}.r2 as r2",
            s"regression_metrics.${predictionLabel}.rmse as rmse",
            s"regression_metrics.${predictionLabel}.mae as mae"
          )
          .collect()(0)
        oneRow.getValuesMap[AnyVal](oneRow.schema.fieldNames)
      } else {
        regressionDefaults()
      }
    } else if (prediction.columns.contains("r2")) {
      val oneRow = prediction
        .selectExpr(
          "r2",
          s"prediction_${label}_rmse as rmse",
          "mae"
        )
        .collect()(0)
      oneRow.getValuesMap[AnyVal](oneRow.schema.fieldNames)
    } else {
      regressionDefaults()
    }
  }

  /**
    * Extract the custom metric from a single row prediction DataFrame
    *
    * @param prediction This is one Row DataFrame
    * @param label
    * @return
    */
  def fetchCustomMetric(
      prediction: DataFrame,
      label: String
  ): Map[String, AnyVal] = {
    if (prediction.columns.contains("custom_metrics")) {
      val tempRow =
        prediction
          .selectExpr(
            "size(custom_metrics) as custom_count"
          )
          .collect()(0)
      val predictionLabel = s"prediction_${label}"
      if (tempRow.getInt(0) > 0) {
        val oneRow = prediction
          .selectExpr(
            s"custom_metrics.${predictionLabel}.bias_avg as bias_avg",
            s"custom_metrics.${predictionLabel}.zero_residuals as zero_residuals",
            s"custom_metrics.${predictionLabel}.count_total as count_total"
          )
          .collect()(0)
        oneRow.getValuesMap[AnyVal](oneRow.schema.fieldNames)
      } else {
        customDefaults()
      }
    } else if (prediction.columns.contains("bias_avg")) {
      val oneRow = prediction
        .selectExpr(
          "bias_avg",
          "zero_residuals",
          "count_total"
        )
        .collect()(0)
      oneRow.getValuesMap[AnyVal](oneRow.schema.fieldNames)
    } else {
      customDefaults()
    }
  }

  /**
    * Extract the classification metric from a single row prediction DataFrame
    * @param prediction This is one Row DataFrame
    * @param label
    * @return
    */
  def fetchClassificationMetric(
      prediction: DataFrame,
      label: String
  ): Map[String, AnyVal] = {
    if (prediction.columns.contains("classification_metrics")) {
      val tempRow =
        prediction
          .selectExpr("size(classification_metrics) as count")
          .collect()(0)
      val predictionLabel = s"prediction_${label}"
      if (tempRow.getInt(0) > 0) {
        val oneRow = prediction
          .selectExpr(
            s"classification_metrics.${predictionLabel}.accuracy as accuracy",
            s"classification_metrics.${predictionLabel}.f1 as f1",
            s"classification_metrics.${predictionLabel}.aucROC as aucROC",
            s"classification_metrics.${predictionLabel}.aucPR as aucPR",
            s"classification_metrics.${predictionLabel}.weightedPrecision as weightedPrecision",
            s"classification_metrics.${predictionLabel}.weightedRecall as weightedRecall"
          )
          .collect()(0)
        oneRow.getValuesMap[AnyVal](oneRow.schema.fieldNames)
      } else {
        classificationDefaults()
      }
    } else if (prediction.columns.contains("accuracy")) {
      val oneRow = prediction
        .select(
          "accuracy",
          "f1",
          "aucROC",
          "aucPR",
          "weightedPrecision",
          "weightedRecall"
        )
        .collect()(0)
      oneRow.getValuesMap[AnyVal](oneRow.schema.fieldNames)
    } else {
      classificationDefaults()
    }
  }

  private val classificationDefaults: () => Map[String, Double] =
    () =>
      Map(
        "accuracy" -> -1.0,
        "f1" -> -1.0,
        "aucROC" -> -1.0,
        "aucPR" -> -1.0,
        "weightedPrecision" -> -1.0,
        "weightedRecall" -> -1.0
      )

  private val regressionDefaults: () => Map[String, Double] =
    () =>
      Map(
        "r2" -> -1.0,
        "rmse" -> -1.0,
        "mae" -> -1.0
      )
  private val customDefaults: () => Map[String, Double] =
    () =>
      Map(
        "bias_avg" -> -1.0,
        "zero_residuals" -> -1.0,
        "count_total" -> -1.0
      )

  def getRandomNSample(inputDF: DataFrame, n: Int = 1000000): DataFrame = {
    val count = inputDF.count()
    val howManyTake = if (count > n) n else count
    inputDF
      .sample(withReplacement = false, fraction = 1.0 * howManyTake / count)
      .limit(n)
      .toDF()
  }

  /**
    * Check transform validity and derive the output schema from the input schema.
    *
    * We check validity for interactions between parameters during `transformSchema` and
    * raise an exception if any parameter value is invalid. Parameter value checks which
    * do not depend on other parameters are handled by `Param.validate()`.
    *
    * Typical implementation should first conduct verification on schema change and parameter
    * validity, including complex parameter interaction checks.
    */
  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  /**
    * Creates a copy of this instance.
    * Requirements:
    *  - The copy must have the same UID.
    *  - The copy must have the same Params, with some possibly overwritten by the `extra`
    *    argument.
    *  - This should do a deep copy of any data members which are mutable.  That said,
    *    Transformers should generally be immutable (except for Params), so the `defaultCopy`
    *    method often suffices.
    * @param extra  Param values which will overwrite Params in the copy.
    */
  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

object ModelMetaTransformer
    extends DefaultParamsReadable[ModelMetaTransformer] {
  override def load(path: String): ModelMetaTransformer = super.load(path)
}
