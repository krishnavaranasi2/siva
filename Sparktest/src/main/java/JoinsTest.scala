
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache._
object JoinsTest {
  val conf = new SparkConf().setAppName("appName").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val rdd1 = sc.parallelize(Seq(
    ("math", 55),
    ("math", 56),
    ("english", 57),
    ("english", 58),
    ("science", 59),
    ("science", 54)))

  val jj = rdd1.coalesce(2)
  println("#######################    ", jj.getNumPartitions)

  val rdd2 = sc.parallelize(Seq(
    ("math", 60),
    ("math", 65),
    ("science", 61),
    ("science", 62),
    ("history", 63),
    ("history", 64)))

  val joined = rdd1.join(rdd2)
  //joined.foreach(println)
  val leftOuterJoin = rdd1.leftOuterJoin(rdd2)
  //leftOuterJoin.foreach(println)
  val rightOuterJoin = rdd1.rightOuterJoin(rdd2)
  rightOuterJoin.foreach(println)
  // group by key

  val grouped1 = rdd1.groupByKey
  grouped1.foreach(println)

  val grouped2 = rdd1.groupBy { x =>
    /*if((x._2 % 2) == 0) {
                              "evennumbers"
                           }else {
                              "oddnumbers"
                           }*/
    x._2
  }

  grouped2.foreach(println)

  val rdd3 = sc.parallelize(List(
    ("maths", 80),
    ("science", 90)))

  println(rdd1.partitions.length)

  val additionalMarks = ("extra", 2)

  val sum = rdd3.fold(additionalMarks) { (acc, marks) =>

    println("acc>>>>>>>>", acc._2)
    println("marks>>>>>>>>>>>>>>>>", marks._2)
    val sum = acc._2 + marks._2
    println("sum >>>>>>>>> ", sum)
    ("total", sum)
  }
  // println("final sum : ", sum)

  // Union

  val rdd4 = sc.parallelize(List("lion", "tiger", "tiger", "peacock", "horse"))

  val rdd5 = sc.parallelize(List("lion", "tiger"))

  val x = rdd4.distinct().collect()
  x.foreach(println)
  val y = rdd4.union(rdd5).collect()
  y.foreach(println)
  val z = rdd4.intersection(rdd5).collect();
  z.foreach(println)
  val cc = rdd4.subtract(rdd5).collect()
  cc.foreach(println)
  val er = rdd4.cartesian(rdd5).collect();
  er.foreach(println)

  val rdd7 = sc.parallelize(List(1, 2, 3, 4, 5), 1)
  rdd7.fold(3){
    (a,b)=>
      println("a : ",a)
      println("b : ",b)
      println("a+b",a+b)
      a+b
  }
  println(rdd7.fold(3)(_ + _))

}


