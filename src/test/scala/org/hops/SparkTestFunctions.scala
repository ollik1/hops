package org.hops

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Test functions for spark suites.
  */
trait SparkTestFunctions { this: SharedSparkContext =>
    /**
      * A test helper to create a RDD with explicit partitions.
      * At least one partition is required.
      *
      * @param p The elements of the first partition.
      * @param ps A sequence of elements of partitions after the first partition.
      *           Can be zero or more.
      * @tparam T Type of the RDD.
      * @return The result RDD with partitions as the given elements.
      */
    def rdd[T : ClassTag](p: List[T], ps: List[T]*): RDD[T] = {
        new RDD[T](sc, List.empty) {
            override def compute(split: Partition, context: TaskContext): Iterator[T] = {
                split.index match {
                    case 0 => p.iterator
                    case idx if idx > 0 => ps(idx - 1).iterator
                }
            }

            override protected def getPartitions: Array[Partition] = {
                (0 to ps.size).toArray.map(idx => new Partition {
                    override def index: Int = idx
                })
            }
        }
    }
}
