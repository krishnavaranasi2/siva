
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache._
import org.apache.spark.HashPartitioner

object WordCountHashPartitioner extends App {
  val conf = new SparkConf().setAppName("appName").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val textFile = sc.textFile("C:\\Users\\sample.txt")

  val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).partitionBy(new HashPartitioner(10))
  println(counts.partitioner)
  val cont = counts.reduceByKey(_ + _)
  

  cont.foreach(println)
}