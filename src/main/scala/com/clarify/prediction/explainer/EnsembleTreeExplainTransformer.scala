package com.clarify.prediction.explainer

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.{SQLDataTypes, Vectors}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{
  DefaultParamsReadable,
  DefaultParamsWritable,
  Identifiable
}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.{
  DataTypes,
  IntegerType,
  StructField,
  StructType
}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.immutable.Nil
class EnsembleTreeExplainTransformer(override val uid: String)
    extends Transformer
    with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("EnsembleTreeExplainTransformer"))

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

  final def setPredictionView(value: String): EnsembleTreeExplainTransformer =
    set(predictionView, value)

  /**
    * Param for coefficientView name.
    */
  final val coefficientView: Param[String] =
    new Param[String](this, "coefficientView", "input coefficient view name")

  final def getCoefficientView: String = $(coefficientView)

  final def setCoefficientView(value: String): EnsembleTreeExplainTransformer =
    set(coefficientView, value)

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

  final def setLabel(value: String): EnsembleTreeExplainTransformer =
    set(label, value)

  // (Optional) You can set defaults for Param values if you like.
  setDefault(
    predictionView -> "predictions",
    coefficientView -> "coefficient",
    label -> "test"
  )

  /**
    * To set nested array(val) schema
    * @param df
    * @param columnName
    * @return
    */
  private def getSchema(df: DataFrame, columnName: String): StructType = {
    var schema: StructType = df.schema
    schema = schema.add(
      columnName,
      DataTypes.createArrayType(
        StructType(
          StructField("feature_number", IntegerType) ::
            StructField("inclusion_index", IntegerType) ::
            StructField("inclusion_path", SQLDataTypes.VectorType) ::
            StructField("exclusion_path", SQLDataTypes.VectorType) :: Nil
        )
      ),
      false
    )
    schema
  }

  /**
    * The encoder applies the schema based on nested vs flattened
    * @param df
    * @param columnName act as prefix when flattened mode else column name when nested mode
    * @return
    */
  private def buildEncoder(
      df: DataFrame,
      columnName: String
  ): ExpressionEncoder[Row] = {
    val newSchema = getSchema(df, columnName)
    RowEncoder.apply(newSchema)
  }

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

    val featureImportanceDF = dataset.sqlContext
      .table($(coefficientView))
      .select("Feature_Index", "Feature", "Coefficient")
      .orderBy("Feature_Index")
      .collect()

    val featureIndexCoefficient = featureImportanceDF
      .map(row => row.getAs[Int](0) -> row.getAs[Double](2))
      .toMap
    val featureIndexName = featureImportanceDF
      .map(row => row.getAs[Int](0) -> row.getAs[String](1))
      .toMap

    val predictionsDf = dataset.sqlContext.table($(predictionView))
    // dataset.toDF().createOrReplaceTempView($(predictionView))

    val newDF =
      pathGenerator(predictionsDf, featureIndexCoefficient, featureIndexName)

    newDF
  }

  /**
    * This is the main entry point to calculate linear contribution of each feature
    * @param df
    * @param featureIndexCoefficient
    * @param featureIndexName
    * @return
    */
  private def pathGenerator(
      df: DataFrame,
      featureIndexCoefficient: Map[Int, Double],
      featureIndexName: Map[Int, String]
  ): DataFrame = {
    val encoder =
      buildEncoder(df, "paths")
    val func =
      pathGeneratorRow(df.schema)(featureIndexCoefficient, featureIndexName)

    df.mapPartitions(x => x.map(func))(encoder)
  }

  /*
    Map over Rows and features to calculate linear contribution of each feature flattened and nested mode
    ----------------------------------------------------------------------
   */
  private val pathGeneratorRow
      : (StructType) => (Map[Int, Double], Map[Int, String]) => Row => Row =
    (schema) =>
      (featureIndexCoefficient, featureIndexName) =>
        (row) => {
          val calculatedPaths = featureIndexCoefficient.map {
            case (outerFeatureNum, outerCoefficient) =>
              val featureName = featureIndexName.get(outerFeatureNum)
              val featureVal =
                row.get(schema.fieldIndex(featureName.get)).toString.toDouble
              if (featureVal > 0) {
                val exclusionPath = featureIndexCoefficient.map {
                  case (_, coefficient) =>
                    if (coefficient <= outerCoefficient) 0.0 else featureVal
                }.toArray
                val inclusionPath = featureIndexCoefficient.map {
                  case (_, coefficient) =>
                    if (coefficient < outerCoefficient) 0.0 else featureVal
                }.toArray
                Row(
                  outerFeatureNum,
                  1,
                  Vectors.dense(inclusionPath).toSparse,
                  Vectors.dense(exclusionPath).toSparse
                )
              } else {
                Row(
                  outerFeatureNum,
                  0,
                  Vectors
                    .sparse(featureIndexCoefficient.size, Array(), Array()),
                  Vectors.sparse(featureIndexCoefficient.size, Array(), Array())
                )
              }
          }.toList
          Row.merge(row, Row(calculatedPaths))
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

object EnsembleTreeExplainTransformer
    extends DefaultParamsReadable[EnsembleTreeExplainTransformer] {
  override def load(path: String): EnsembleTreeExplainTransformer =
    super.load(path)
}
