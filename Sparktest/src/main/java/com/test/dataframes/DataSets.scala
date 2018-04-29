package com.test.dataframes
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.Column
import java.util.Date

object DataSets extends App {
    val spark = SparkSession
        .builder()
        .appName("Spark SQL basic example")
        .config("spark.some.config.option", "some-value").master("local")
        .getOrCreate()
    import spark.implicits._
    val df = spark.read.options(Map(
        "header" -> "true",
        "inferSchema" -> "true",
        "nullValue" -> "NA",
        "timestampFormat" -> "yyyy-MM-dd'T'HH:mm​:ss")).csv("F:\\employee.csv")
    case class Person(age: Long, name: String)
    val df1 = spark.read.json("F:\\people.json")
    df1.foreach(f => println(f(0),f(1)))

    df.printSchema()
    case class Selection(Age: Integer, remote_work: String, leave: String)
    val k = df.select("Age", "remote_work", "leave").as[Selection]
    k.collect()

}