package com.test.dataframes
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object JoinsDataFrame extends App {
   /* val spark = SparkSession
        .builder()
        .appName("Spark SQL basic example")
        .config("spark.some.config.option", "some-value").master("local")
        .getOrCreate()
    import spark.implicits._*/
    /* val employees = spark.sparkContext.parallelize[(String,Option[In])](Array(
        ("Rafferty", 31), ("Jones", 33), ("Heisenberg", 33), ("Robinson", 34), ("Smith", 34), ("Williams", null))).toDF("LastName", "DepartmentID")

    val departments = spark.sparkContext.parallelize(Array(
        (31, "Sales"), (33, "Engineering"), (34, "Clerical"),
        (35, "Marketing"))).toDF("DepartmentID", "DepartmentName")*/

    // Inner join implicit
    // employees.join(departments, employees("DepartmentID") === departments("DepartmentID"))
    // Inner join explicit
    //employees.join(departments, employees("DepartmentID") === departments("DepartmentID"), "inner").show()

    // Left outer join explicit
    //employees.join(departments, employees("DepartmentID") === departments("DepartmentID"), "left_outer").show()

    /*
     def loadFile[T](file: String)(implicit mf: Manifest[T]): RDD[T] = {
    if (file == null) {
      return null;
    }
  spark.sparkContext.textFile(file, 1).map { line => JSONUtils.deserialize[T](line) }.filter { x => x != null }.cache();
  }*/
    val x = "100".asInstanceOf[String]
    val k =Integer.parseInt(x)
    println(k)

 /*   val kkk: Option[Integer] = None
    
   val test =  kkk

    println(kkk.get)*/

}