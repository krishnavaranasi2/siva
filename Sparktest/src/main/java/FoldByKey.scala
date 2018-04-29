import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache._
import org.apache.spark.HashPartitioner
import org.apache.spark.RangePartitioner

object FoldByKey extends App {
    val conf = new SparkConf().setAppName("appName").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val deptEmployees = List(
        ("cs", ("jack", 1000.0)),
        ("cs", ("bron", 1200.0)),
        ("phy", ("sam", 2200.0)),
        ("phy", ("ronaldo", 500.0)))

    val employeeRDD = sc.makeRDD(deptEmployees) //.partitionBy(new HashPartitioner(10))

    val maxByDept = employeeRDD.foldByKey(("dummy", 0.0))((acc, element) => if (acc._2 > element._2) acc else element)

    println("maximum salaries in each dept" + maxByDept.collect().toList)

    val maxByDeptReduceByKey = employeeRDD.reduceByKey((acc, element) => if (acc._2 > element._2) acc else element)
    println("maximum salaries in each dept" + maxByDeptReduceByKey.collect().toList)

    val babyNamesCSV = sc.parallelize(List(("David", 6), ("Abby", 4), ("David", 5), ("Abby", 5)))
    babyNamesCSV.reduceByKey((n, c) => n + c).collect

    babyNamesCSV.aggregateByKey(0)(
        (k, v) =>
            v + k,

        (v, k) => k + v)

    val rdd1 = sc.parallelize(List(
        ("maths", 80),
        ("science", 90)))

    val additionalMarks = ("extra", 1)

    val sum = rdd1.fold(additionalMarks) { (acc, marks) =>
        val sum = acc._2 + marks._2
        ("total", sum)
    }

    println(sum)
    
}