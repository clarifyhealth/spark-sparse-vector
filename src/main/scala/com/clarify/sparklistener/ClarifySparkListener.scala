package com.clarify.sparklistener

import org.apache.spark.scheduler._

class ClarifySparkListener extends SparkListener {
  private val SPARK_JOB_DESCRIPTION = "spark.job.description"
  private val SPARK_JOB_GROUP_ID = "spark.jobGroup.id"

  override def onJobStart(jobStart: SparkListenerJobStart) {
    println(s"[ClarifySparkListener] Job ${jobStart.jobId} started with ${jobStart.stageInfos.size}")
    for (stage <- jobStart.stageInfos) {
      println(s"id=${stage.stageId} name=${stage.name} tasks=${stage.numTasks}")
    }
    val lastStageInfo = jobStart.stageInfos.sortBy(_.stageId).lastOption
    val jobName = lastStageInfo.map(_.name).getOrElse("")
    val jobGroup = Option(jobStart.properties)
      .flatMap { p => Option(p.getProperty(SPARK_JOB_GROUP_ID)) }
    val jobDescription = Option(jobStart.properties)
      .flatMap { p => Option(p.getProperty(SPARK_JOB_DESCRIPTION)) }
    println(s"jobName=$jobName description=$jobDescription group=$jobGroup")
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    println(s"[ClarifySparkListener] Job ${jobEnd.jobId} ended with ${jobEnd.jobResult} ${jobEnd.time}")
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    val description = Option(stageSubmitted.properties).flatMap { p =>
      Option(p.getProperty(SPARK_JOB_DESCRIPTION))
    }
    println(s"[ClarifySparkListener] Stage Submitted ${stageSubmitted.stageInfo.stageId} ${stageSubmitted.stageInfo.name} desc=$description ${stageSubmitted.stageInfo.name}")
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    println(s"[ClarifySparkListener] Stage completed ${stageCompleted.stageInfo.stageId} completed with ${stageCompleted.stageInfo.numTasks} tasks.")
    println(s"[ClarifySparkListener]  Stage completed, runTime: ${stageCompleted.stageInfo.taskMetrics.executorRunTime}, " +
      s"cpuTime: ${stageCompleted.stageInfo.taskMetrics.executorCpuTime}")

  }
}
