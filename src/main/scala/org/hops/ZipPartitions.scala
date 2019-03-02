package org.hops

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}
import shapeless.ops.function.FnToProduct
import shapeless.ops.hlist.{IsHCons, Mapper, ZipConst}
import shapeless.syntax.std.function._
import shapeless.{HList, Poly1}

import scala.reflect.ClassTag

object ZipPartitions {

    /**
      * A polymorphic function to map a partition of a RDD to a corresponding iterator.
      */
    object RddToIterator extends Poly1 {
        implicit def default[T]: RddToIterator.Case[(RDD[T], (Partition, TaskContext))] {
            type Result = Iterator[T]
        } = at[(RDD[T], (Partition, TaskContext))] {
            case (rdd, (partition, taskContext)) =>
                rdd.iterator(partition, taskContext)
        }
    }

    /**
      * Given a non-empty list of RDDs with equal partitioning, zips partitions into a single RDD
      * using a given function.
      */
    def apply[L <: HList, A0 <: HList, A1 <: HList, R : ClassTag, H <: RDD[_], T <: HList, I, F](rdds: L)(f: F)(
        implicit ev0: IsHCons.Aux[L, H, T],
        ev1: ZipConst.Aux[(Partition, TaskContext), L, A0],
        ev2: Mapper.Aux[RddToIterator.type, A0, A1],
        ev3: FnToProduct.Aux[F, A1 => I],
        ev4: I <:< Iterator[R]): RDD[R] = {

        val sc = rdds.head.sparkContext

        new RDD[R](sc, List.empty) {
            override def compute(split: Partition, context: TaskContext): Iterator[R] = {
                f.toProduct(rdds.zipConst((split, context)).map(RddToIterator))

            }

            override protected def getPartitions: Array[Partition] = rdds.head.partitions
        }
    }
}
