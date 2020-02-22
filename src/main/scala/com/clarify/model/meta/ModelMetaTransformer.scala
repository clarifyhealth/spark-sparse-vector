package com.clarify.model.meta

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{
  DefaultParamsReadable,
  DefaultParamsWritable,
  Identifiable
}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}
class ModelMetaTransformer(override val uid: String)
    extends Transformer
    with DefaultParamsWritable {

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
  final val label: Param[String] =
    new Param[String](
      this,
      "label",
      "training label name"
    )

  final def getLabel: String = $(label)

  final def setLabel(value: String): ModelMetaTransformer =
    set(label, value)

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

  // (Optional) You can set defaults for Param values if you like.
  setDefault(
    predictionView -> "predictions",
    coefficientView -> "coefficient",
    modelMetaView -> "modelMeta",
    linkFunction -> "other",
    label -> "test",
    family -> "gaussian",
    linkPower -> 0.0,
    variancePower -> -1.0
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
    dataset.toDF()
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
