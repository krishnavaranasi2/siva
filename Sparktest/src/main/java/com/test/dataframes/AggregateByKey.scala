package com.test.dataframes
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
object AggregateByKey {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("appName").setMaster("local[*]")
        val sc = new SparkContext(conf)
        val keysWithValuesList = Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C", "bar=D", "bar=D")
        val data = sc.parallelize(keysWithValuesList)
        //Create key value pairs
        val kv = data.map(_.split("=")).map(v => (v(0), v(1))).cache()

        val initialCount = 0;
        val addToCounts = (n: Int, v: String) =>{ println(n+1) 
            n + 1}
        val sumPartitionCounts = (p1: Int, p2: Int) => {
            
            println(p1, "+",p2)
            p1 + p2
        }
        println(initialCount, addToCounts, sumPartitionCounts)
        val countByKey = kv.aggregateByKey(initialCount)(addToCounts, sumPartitionCounts).collect

    }
}