package com.clarify.sparklistener

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart, SparkListenerStageCompleted}

class ClarifySparkListener extends SparkListener {
  override def onJobStart(jobStart: SparkListenerJobStart) {
    println(s"[ClarifySparkListener] Job started with ${jobStart.stageInfos.size} stages: $jobStart")
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    println(s"[ClarifySparkListener] Stage ${stageCompleted.stageInfo.stageId} completed with ${stageCompleted.stageInfo.numTasks} tasks.")
  }
}
