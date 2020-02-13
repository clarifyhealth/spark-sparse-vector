package com.clarify.sparklistener

import java.io.PrintStream

import org.apache.spark.util.StatCounter

import scala.collection.immutable
import scala.collection.immutable.IndexedSeq

/**
 * Util for getting some stats from a small sample of numeric values, with some handy
 * summary functions.
 *
 * Entirely in memory, not intended as a good way to compute stats over large data sets.
 *
 * Assumes you are giving it a non-empty set of data
 */
//noinspection TypeAnnotation
private class Distribution(val data: Array[Double], val startIdx: Int, val endIdx: Int) {
  require(startIdx < endIdx)

  def this(data: Iterable[Double]) = this(data.toArray, 0, data.size)

  java.util.Arrays.sort(data, startIdx, endIdx)
  val length = endIdx - startIdx

  val defaultProbabilities = Array(0, 0.25, 0.5, 0.75, 1.0)

  /**
   * Get the value of the distribution at the given probabilities.  Probabilities should be
   * given from 0 to 1
   *
   * @param probabilities probabilities
   */
  //noinspection SpellCheckingInspection
  def getQuantiles(probabilities: Iterable[Double] = defaultProbabilities)
  : IndexedSeq[Double] = {
    probabilities.toIndexedSeq.map { p: Double => data(closestIndex(p)) }
  }

  private def closestIndex(p: Double) = {
    math.min((p * length).toInt + startIdx, endIdx - 1)
  }

  def showQuantiles(out: PrintStream = System.out): Unit = {
    // scalastyle:off println
    out.println("min\t25%\t50%\t75%\tmax")
    getQuantiles(defaultProbabilities).foreach { q => out.print(q + "\t") }
    out.println()
    // scalastyle:on println
  }

  def statCounter: StatCounter = StatCounter(data.slice(startIdx, endIdx))

  /**
   * print a summary of this distribution to the given PrintStream.
   *
   * @param out out
   */
  def summary(out: PrintStream = System.out): Unit = {
    // scalastyle:off println
    out.println(statCounter)
    showQuantiles(out)
    // scalastyle:on println
  }

  def hasSkew: Boolean = {
    skewness.abs > 0.5
  }

  def skewness: Double = {
    if (statCounter.count > 5) {
      val quantiles: immutable.Seq[Double] = getQuantiles(defaultProbabilities)
      //    println("quantiles:" + quantiles)
      val mean: Double = statCounter.mean
      val std_dev: Double = statCounter.sampleStdev
      val median: Double = quantiles(2)
      //    println("mean:" + mean + ", std_dev:" + std_dev + ", median:" + median)
      val skewness = if (std_dev == 0 || std_dev.isNaN) 0 else 3 * (mean - median) / std_dev
      skewness
    }
    else {
      0
    }
  }
}

private object Distribution {

  def apply(data: Iterable[Double]): Option[Distribution] = {
    if (data.nonEmpty) {
      Some(new Distribution(data))
    } else {
      None
    }
  }

  def showQuantiles(out: PrintStream = System.out, quantiles: Iterable[Double]): Unit = {
    // scalastyle:off println
    out.println("min\t25%\t50%\t75%\tmax")
    quantiles.foreach { q => out.print(q + "\t") }
    out.println()
    // scalastyle:on println
  }
}
