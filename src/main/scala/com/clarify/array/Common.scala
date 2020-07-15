package com.clarify.array

object Common {
  def generateKernel(window: Int): Array[Double] = {
    1 to window map (_ => 1.0 / window) toArray
  }
}
