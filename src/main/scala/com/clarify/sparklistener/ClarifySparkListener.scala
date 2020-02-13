package com.clarify.sparklistener

import java.math.{MathContext, RoundingMode}
import java.util.Locale

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._

import scala.collection.mutable


/**
 * :: DeveloperApi ::
 * Simple SparkListener that logs a few summary statistics when each stage completes.
 */
class ClarifySparkListener extends SparkListener with Logging {

  import com.clarify.sparklistener.StatsReportListener._

  private val taskInfoMetrics = mutable.Buffer[(TaskInfo, TaskMetrics)]()

  private val SPARK_JOB_DESCRIPTION = "spark.job.description"
  private val SPARK_JOB_GROUP_ID = "spark.jobGroup.id"

  override def onJobStart(jobStart: SparkListenerJobStart) {
    val lastStageInfo = jobStart.stageInfos.sortBy(_.stageId).lastOption
    val jobName = lastStageInfo.map(_.name).getOrElse("")
    val jobGroup = Option(jobStart.properties)
      .flatMap { p => Option(p.getProperty(SPARK_JOB_GROUP_ID)) }
    val jobDescription = Option(jobStart.properties)
      .flatMap { p => Option(p.getProperty(SPARK_JOB_DESCRIPTION)) }
    println(s"[ClarifySparkListener] Job ${jobStart.jobId} jobName=$jobName description=$jobDescription group=$jobGroup"
      + s" started with ${jobStart.stageInfos.size} tasks:")
    for (stage <- jobStart.stageInfos) {
      println(s"[ClarifySparkListener] Task id=${stage.stageId} name=${stage.name} tasks=${stage.numTasks}")
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    println(s"[ClarifySparkListener] Job ${jobEnd.jobId} ended with ${jobEnd.jobResult} ${jobEnd.time}")
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val description = Option(stageSubmitted.properties).flatMap { p =>
      Option(p.getProperty(SPARK_JOB_DESCRIPTION))
    }
    println(s"[ClarifySparkListener] Stage ${stageSubmitted.stageInfo.stageId} Submitted ${stageSubmitted.stageInfo.name}"
      + s" desc=$description ${stageSubmitted.stageInfo.name}")
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val info = taskEnd.taskInfo
    val metrics = taskEnd.taskMetrics
    if (info != null && metrics != null) {
      taskInfoMetrics += ((info, metrics))
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    implicit val sc: SparkListenerStageCompleted = stageCompleted
    println(s"[ClarifySparkListener] Finished stage: ${getStatusDetail(stageCompleted.stageInfo)}")
    showMillisDistribution("task runtime:", (info, _) => info.duration, taskInfoMetrics)

    // Shuffle write
    showBytesDistribution("shuffle bytes written:",
      (_, metric) => metric.shuffleWriteMetrics.bytesWritten, taskInfoMetrics)

    // Fetch & I/O
    showMillisDistribution("fetch wait time:",
      (_, metric) => metric.shuffleReadMetrics.fetchWaitTime, taskInfoMetrics)
    showBytesDistribution("remote bytes read:",
      (_, metric) => metric.shuffleReadMetrics.remoteBytesRead, taskInfoMetrics)
    showBytesDistribution("task result size:",
      (_, metric) => metric.resultSize, taskInfoMetrics)

    // Runtime breakdown
    val runtimePcts = taskInfoMetrics.map { case (info, metrics) =>
      RuntimePercentage(info.duration, metrics)
    }
//    showDistribution("executor (non-fetch) time pct: ",
//      Distribution(runtimePcts.map(_.executorPct * 100)), "%2.0f %%")
//    showDistribution("fetch wait time pct: ",
//      Distribution(runtimePcts.flatMap(_.fetchPct.map(_ * 100))), "%2.0f %%")
//    showDistribution("other time pct: ", Distribution(runtimePcts.map(_.other * 100)), "%2.0f %%")
    taskInfoMetrics.clear()
  }

  private def getStatusDetail(info: StageInfo): String = {
    val failureReason = info.failureReason.map("(" + _ + ")").getOrElse("")
    val timeTaken = info.submissionTime.map(
      x => info.completionTime.getOrElse(System.currentTimeMillis()) - x
    ).getOrElse("-")

    s"[ClarifySparkListener] Stage(${info.stageId}, ${info.attemptNumber}); Name: '${info.name}'; " +
      //      s"Status: ${info.getStatusString}$failureReason; numTasks: ${info.numTasks}; " +
      s"Took: $timeTaken msec"
  }

}

private object StatsReportListener extends Logging {

  // For profiling, the extremes are more interesting
  val percentiles: Array[Int] = Array[Int](0, 5, 10, 25, 50, 75, 90, 95, 100)
  val probabilities: Array[Double] = percentiles.map(_ / 100.0)
  val percentilesHeader: String = "\t" + percentiles.mkString("%\t") + "%"

  def extractDoubleDistribution(
                                 taskInfoMetrics: Seq[(TaskInfo, TaskMetrics)],
                                 getMetric: (TaskInfo, TaskMetrics) => Double): Option[Distribution] = {
    Distribution(taskInfoMetrics.map { case (info, metric) => getMetric(info, metric) })
  }

  // Is there some way to setup the types that I can get rid of this completely?
  def extractLongDistribution(
                               taskInfoMetrics: Seq[(TaskInfo, TaskMetrics)],
                               getMetric: (TaskInfo, TaskMetrics) => Long): Option[Distribution] = {
    extractDoubleDistribution(
      taskInfoMetrics,
      (info, metric) => {
        getMetric(info, metric).toDouble
      })
  }

  def showDistribution(heading: String, d: Distribution, formatNumber: Double => String) {
    val stats = d.statCounter
    val quantiles = d.getQuantiles(probabilities).map(formatNumber)
    println("[ClarifySparkListener] showDistribution: "+ heading + stats)
    println("skewness:" + d.skewness)
    if (d.hasSkew) {
      println("===== WARNING: HasSkew:" + d.hasSkew + " median=" )
    }

    println(percentilesHeader)
    println("\t" + quantiles.mkString("\t"))
  }

  def showDistribution(
                        heading: String,
                        dOpt: Option[Distribution],
                        formatNumber: Double => String) {
    dOpt.foreach { d => showDistribution(heading, d, formatNumber) }
  }

  def showDistribution(heading: String, dOpt: Option[Distribution], format: String) {
    def f(d: Double): String = format.format(d)

    showDistribution(heading, dOpt, f _)
  }

  def showDistribution(
                        heading: String,
                        format: String,
                        getMetric: (TaskInfo, TaskMetrics) => Double,
                        taskInfoMetrics: Seq[(TaskInfo, TaskMetrics)]) {
    showDistribution(heading, extractDoubleDistribution(taskInfoMetrics, getMetric), format)
  }

  def showBytesDistribution(
                             heading: String,
                             getMetric: (TaskInfo, TaskMetrics) => Long,
                             taskInfoMetrics: Seq[(TaskInfo, TaskMetrics)]) {
    showBytesDistribution(heading, extractLongDistribution(taskInfoMetrics, getMetric))
  }

  def showBytesDistribution(heading: String, dOpt: Option[Distribution]) {
    dOpt.foreach { dist => showBytesDistribution(heading, dist) }
  }

  def showBytesDistribution(heading: String, dist: Distribution) {
        showDistribution(heading, dist, (d => bytesToString(d.toLong)): Double => String)
  }

  def showMillisDistribution(heading: String, dOpt: Option[Distribution]) {
    showDistribution(heading, dOpt,
      (d => StatsReportListener.millisToString(d.toLong)): Double => String)
  }

  def showMillisDistribution(
                              heading: String,
                              getMetric: (TaskInfo, TaskMetrics) => Long,
                              taskInfoMetrics: Seq[(TaskInfo, TaskMetrics)]) {
    showMillisDistribution(heading, extractLongDistribution(taskInfoMetrics, getMetric))
  }

  val seconds = 1000L
  val minutes: Long = seconds * 60
  val hours: Long = minutes * 60

  /**
   * Reformat a time interval in milliseconds to a prettier format for output
   */
  def millisToString(ms: Long): String = {
    val (size, units) =
      if (ms > hours) {
        (ms.toDouble / hours, "hours")
      } else if (ms > minutes) {
        (ms.toDouble / minutes, "min")
      } else if (ms > seconds) {
        (ms.toDouble / seconds, "s")
      } else {
        (ms.toDouble, "ms")
      }
    "%.1f %s".format(size, units)
  }

  /**
   * Convert a quantity in bytes to a human-readable string such as "4.0 MiB".
   */
  def bytesToString(size: Long): String = bytesToString(BigInt(size))

  def bytesToString(size: BigInt): String = {
    val EiB = 1L << 60
    val PiB = 1L << 50
    val TiB = 1L << 40
    val GiB = 1L << 30
    val MiB = 1L << 20
    val KiB = 1L << 10

    if (size >= BigInt(1L << 11) * EiB) {
      // The number is too large, show it in scientific notation.
      BigDecimal(size, new MathContext(3, RoundingMode.HALF_UP)).toString() + " B"
    } else {
      val (value, unit) = {
        if (size >= 2 * EiB) {
          (BigDecimal(size) / EiB, "EiB")
        } else if (size >= 2 * PiB) {
          (BigDecimal(size) / PiB, "PiB")
        } else if (size >= 2 * TiB) {
          (BigDecimal(size) / TiB, "TiB")
        } else if (size >= 2 * GiB) {
          (BigDecimal(size) / GiB, "GiB")
        } else if (size >= 2 * MiB) {
          (BigDecimal(size) / MiB, "MiB")
        } else if (size >= 2 * KiB) {
          (BigDecimal(size) / KiB, "KiB")
        } else {
          (BigDecimal(size), "B")
        }
      }
      "%.1f %s".formatLocal(Locale.US, value, unit)
    }
  }

}

private case class RuntimePercentage(executorPct: Double, fetchPct: Option[Double], other: Double)

private object RuntimePercentage {
  def apply(totalTime: Long, metrics: TaskMetrics): RuntimePercentage = {
    val denom = totalTime.toDouble
    val fetchTime = Some(metrics.shuffleReadMetrics.fetchWaitTime)
    val fetch = fetchTime.map(_ / denom)
    val exec = (metrics.executorRunTime - fetchTime.getOrElse(0L)) / denom
    val other = 1.0 - (exec + fetch.getOrElse(0d))
    RuntimePercentage(exec, fetch, other)
  }
}
