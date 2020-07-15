package com.clarify.array

import breeze.linalg.DenseVector
import breeze.signal._
import org.apache.spark.sql.Row
import org.apache.spark.sql.api.java.UDF1

import scala.collection.SortedMap

class ConvolveStruct extends UDF1[Seq[Row], Seq[Double]] {

  override def call(data: Seq[Row]): Seq[Double] = {

    val dataValues = SortedMap(data.map {
      case Row(interval_index: Int, value: Double, is_null: Boolean) =>
        if (is_null) interval_index -> Double.NaN else interval_index -> value
    }: _*).values.toArray

    val dataValuesImputed = data.map {
      case Row(interval_index: Int, value: Double, is_null: Boolean) => {
        if (is_null) {
          lookForValuesAround(interval_index, dataValues) match {
            case (Some(lastKnowGoodBackward), Some(lastKnowGoodForward)) =>
              (lastKnowGoodBackward + lastKnowGoodForward) / 2
            case (Some(lastKnowGoodBackward), None) => lastKnowGoodBackward
            case (Some(lastKnowGoodForward), None)  => lastKnowGoodForward
            case (None, None)                       => dataValues.sum / dataValues.length
          }
        } else {
          value
        }
      }
    }.toArray

    val window = dataValuesImputed.length
    val kernel = Common.generateKernel(window)
    convolve(DenseVector(dataValuesImputed: _*), DenseVector(kernel: _*)).toArray
  }

  def lookForValuesAround(
      interval_index: Int,
      dataValues: Array[Double]
  ): (Option[Double], Option[Double]) = {
    (
      lookBackward(interval_index, dataValues),
      lookForward(interval_index, dataValues)
    )
  }

  def lookBackward(
      interval_index: Int,
      dataValues: Array[Double]
  ): Option[Double] = {
    val index = (interval_index - 1 to 0 by -1).find(x => !dataValues(x).isNaN)
    index match {
      case Some(x) => Some(dataValues(x))
    }
  }

  def lookForward(
      interval_index: Int,
      dataValues: Array[Double]
  ): Option[Double] = {
    val index = (interval_index + 1 until dataValues.length)
      .find(x => !dataValues(x).isNaN)
    index match {
      case Some(x) => Some(dataValues(x))
    }
  }
}
