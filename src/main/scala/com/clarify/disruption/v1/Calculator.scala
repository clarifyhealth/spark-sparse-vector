package com.clarify.disruption.v1

import com.clarify.array.StdDev
import org.apache.spark.sql.api.java.UDF1

import scala.math.pow

class Calculator extends UDF1[Seq[Double], Double] {
  override def call(data: Seq[Double]): Double = {
    // data is the array that we get from the convolution process

    // Calculating the linear regression
    val y_values = data
    val y_values_length = data.length
    val x_values =
      Array.range(-(y_values_length - 1) / 2, (y_values_length - 1) / 2 + 1)

    val sigma_x2 = x_values.map(x => pow(x, 2)).sum
    val sigma_xy = x_values.zip(y_values).map { case (x, y) => x * y }.sum

    val slope = (sigma_xy) / sigma_x2
    val y_mean = y_values.sum / y_values_length
    val linear_predictions = x_values.map(x => slope * x + y_mean)

    val residuals = linear_predictions.zip(y_values).map {
      case (predicted, actual) => predicted - actual
    }

    val residual_stddev = new StdDev().call(residuals)

    residual_stddev

  }
}
