package com.clarify.memory

object MemoryDiagnostics {

  def print_memory_stats(): Unit = {
    // memory info
    val mb = 1024 * 1024
    val runtime = Runtime.getRuntime
    println("** Spark Memory:  Used(MB): "
      + (runtime.totalMemory - runtime.freeMemory) / mb
      + " Free(MB): " + runtime.freeMemory / mb
      + " Total(MB): " + runtime.totalMemory / mb
      + " Max(MB): " + runtime.maxMemory / mb
      + " **")
  }

  val mb: Long = 1024 * 1024

  def print_free_memory(): Unit = {
    // memory info

    val runtime = Runtime.getRuntime
    println("** Free Memory (MB):  " + runtime.freeMemory / mb)
  }

  def get_free_memory(): Long = {
    val runtime = Runtime.getRuntime
    runtime.freeMemory
  }

  def getFreeMemoryMB: Long = {
    val runtime = Runtime.getRuntime
    runtime.freeMemory / mb
  }
}
