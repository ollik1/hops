# HOpS - Heterogenous Operators for Spark

Experimenting with compiler-checked heterogenous argument lists for Spark. For example, zip partitions is defined for 2, 3 and 4 RDDs in Spark API https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD by overloading the function. Using shapeless https://github.com/milessabin/shapeless , we can define one generic function whose signature is checked at compile time.
