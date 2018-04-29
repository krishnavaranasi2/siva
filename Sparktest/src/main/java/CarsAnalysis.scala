
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache._
case class cars(make: String, model: String, mpg: Integer, cylinders: Integer, engine_disp: Integer, horsepower: Integer, weight: Integer, accelerate: Double, year: Integer, origin: String)

object CarsAnalysis extends App {
  val conf = new SparkConf().setAppName("appName").setMaster("local[*]")
  val sc = new SparkContext(conf)
  var rawData = sc.textFile("C:\\Users\\Cars.txt")
  /*val sparkSession = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .enableHiveSupport()
      .getOrCreate()*/
  // map columns and datatypes

  //val data = sc.textFile("path_to_data")
  val header = rawData.first() //#extract header
  rawData = rawData.filter(row => row != header)

  val inputrdd = sc.parallelize(Seq(("maths", 50), ("maths", 60), ("english", 65)))
  //inputrdd: org.apache.spark.rdd.RDD[(String, Int)] = ParallelCollectionRDD[29] at parallelize at :21

  val mapped = inputrdd.map { x => (x._1, x._2) }
  //println(">>>>>>>>>>>>>>.################# ",mapped)

  //mapped.foreach(println)

  val carsData = rawData.map(x => x.split("\t"))
    .map(x => cars(x(0).toString, x(1).toString, x(2).toInt, x(3).toInt, x(4).toInt, x(5).toInt, x(6).toInt, x(7).toDouble, x(8).toInt, x(9).toString))
  carsData.foreach(println)
  carsData.map { x => x.model }
  val cv = carsData.map { x => (x.make, x.cylinders) }
  val kk = cv.mapValues(f => f + 2)
  //carsData.foreach(println)
  // println("carsData.take(2)>>>>>>>>>>>>>>>>>>>>>>")
  val a = carsData.take(1)
  println(">>>>>>>>>>>>>>>>>>>>>. ", a(0))
  //println(a(0))
  //persist to memory
  //carsData.cache()

  //count cars origin wise
  val ccc = carsData.map(x => (x.origin, 1)).reduceByKey((x, y) => x + y).collect
  ccc.foreach(println)
  //filter out american cars
  val americanCars = carsData.filter(x => (x.origin == "American"))

  //count total american cars
  println("americanCars.count()>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
  val d = americanCars.count()
  println(d)

  americanCars.saveAsTextFile("F:\\carsMakeWeightAvg.txt")
}