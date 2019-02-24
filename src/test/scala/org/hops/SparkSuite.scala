package org.hops

import org.scalatest.FunSuite

/**
  * A base-class for test suites using spark.
  */
abstract class SparkSuite extends FunSuite with SharedSparkContext
