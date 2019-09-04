package com.clarify.memory

object MemoryDiagnostics {

  def print_memory_stats(): Unit = {
    // memory info
    val mb = 1024 * 1024
    val runtime = Runtime.getRuntime
    println("** Used Memory (MB):  " + (runtime.totalMemory - runtime.freeMemory) / mb)
    println("** Free Memory (MB):  " + runtime.freeMemory / mb)
    println("** Total Memory (MB): " + runtime.totalMemory / mb)
    println("** Max Memory (MB):   " + runtime.maxMemory / mb)
  }

  def print_free_memory(): Unit = {
    // memory info
    val mb = 1024 * 1024
    val runtime = Runtime.getRuntime
    println("** Free Memory (MB):  " + runtime.freeMemory / mb)
  }

  def get_free_memory(): Long = {
    val runtime = Runtime.getRuntime
    runtime.freeMemory
  }
}
