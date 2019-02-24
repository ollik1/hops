package org.hops

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
  * A simple shared spark context for tests.
  *
  * Replace with https://github.com/holdenk/spark-testing-base
  * when scala 2.12. support gets released.
  */
trait SharedSparkContext extends BeforeAndAfterAll { this: Suite =>
    lazy val sc = {
        val conf = new SparkConf(false)
        new SparkContext("local[4]", "test", conf)
    }

    override protected def beforeAll(): Unit = {
        super.beforeAll()
        sc
    }

    override protected def afterAll(): Unit = {
        sc.stop()
        super.afterAll()
    }
}
