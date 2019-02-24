package org.hops

import org.scalatest.Matchers
import shapeless.{::, HNil}

class ZipPartitionsTest extends SparkSuite with SparkTestFunctions with Matchers {

    test("zip partitions") {
        val rdd1 = rdd(List(1, 2), List(2))
        val rdd2 = rdd(List("ab"), List("cdef"))
        val f: (Iterator[Int] :: Iterator[String] :: HNil) => Iterator[Int] = {
            case x :: y :: HNil => Iterator(x.sum, y.map(_.length).sum)
        }
        ZipPartitions(rdd1 :: rdd2 :: HNil)(f).collect() should contain theSameElementsAs List(3, 2, 2, 4)
    }
}
