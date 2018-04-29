
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache._
import org.apache.spark.HashPartitioner
import org.apache.spark.RangePartitioner

object WordCountRangePartioner extends App {
  val conf = new SparkConf().setAppName("appName").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val textFile = sc.textFile("C:\\Users\\sample.txt")

  val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1))

  val range = counts.partitionBy(new RangePartitioner(10, counts))

  range.reduceByKey(_ + _)
}