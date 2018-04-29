package com.test.dataframes
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
object CombineByKey {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("appName").setMaster("local[*]")
        val sc = new SparkContext(conf)
        val inputrdd = sc.parallelize(
            Seq(
                ("maths", 50), ("maths", 60),
                ("english", 65),
                ("physics", 66), ("physics", 61), ("physics", 87)),
            1)

        val reduced = inputrdd.combineByKey(
            (mark) => {
                println(s"Create combiner -> ${mark}")
                (mark, 1)
            },
            (acc: (Int, Int), v) => {
                println(s"""Merge value : (${acc._1} + ${v}, ${acc._2} + 1)""")
                println(acc, v)
                (acc._1 + v, acc._2 + 1)
            },
            (acc1: (Int, Int), acc2: (Int, Int)) => {
                println(s"""Merge Combiner : (${acc1._1} + ${acc2._1}, ${acc1._2} + ${acc2._2})""")
                (acc1._1 + acc2._1, acc1._2 + acc2._2)
            })
        val result = reduced.mapValues(x => x._1 / x._2.toFloat)
        
        println(result.collect())

    }

}