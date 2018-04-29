package com.test.dataframes
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.Column

object ReadCSVFiles {
    def main(args: Array[String]): Unit = {

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
        // val v = Map(1->"hi",2 ->"yy")
        // v(1)
        df.printSchema()
        //df.show()
        //df.printSchema()
        //df.rdd.getNumPartitions
        df.rdd.getNumPartitions
val kkkk = df.rdd

        val df5 = df.repartition(5).toDF()

        //df5.rdd.getNumPartitions

        df.select("Timestamp", "Age", "remote_work", "leave").filter("Age > 40").show
        val df2 = df.select("Gender", "treatment")
        df2.show()

        //df1.show()

        def parseGender(g: String) = {
            g.toLowerCase match {
                case "male" | "m" | "male-ish" | "maile" |
                    "mal" | "male (cis)" | "make" | "male " |
                    "man" | "msle" | "mail" | "malr" |
                    "cis man" | "cis male" => "Male"
                case "cis female" | "f" | "female" |
                    "woman" | "femake" | "female " |
                    "cis-female/femme" | "female (cis)" |
                    "femail" => "Female"
                case _ => "Transgender"
            }

        }
        import org.apache.spark.sql.functions.udf

        val parseGenderUDF = udf(parseGender _)
        val df6 = df.select(parseGenderUDF($"Gender").alias("Gender"), $"treatment")
        df6.show
        //Write Data Frame to Parquet
        /*df6.write
        .format("parquet")
        .mode("overwrite")
        .save("F://Parquet")
*/
        val df1 = spark.read.json("F:\\people.json")
        df1.show()
        // Select everybody, but increment the age by 1
        df1.select($"name", $"age" + 1).show()

        // Select people older than 21
        //df1.filter($"age" > 21).show()

        // Count people by age
        df.groupBy("age").count().show()

        // Register the DataFrame as a SQL temporary view
        df.createOrReplaceTempView("people")

        val sqlDF = spark.sql("SELECT * FROM people")
        sqlDF.show()

        // Register the DataFrame as a global temporary view
        df.createGlobalTempView("people")

        // Global temporary view is tied to a system preserved database `global_temp`
        spark.sql("SELECT * FROM global_temp.people").show()

        // Global temporary view is cross-session
        spark.newSession().sql("SELECT * FROM global_temp.people").show()

        //word count in data frame
        //val df9 = spark.read.text("C:\\Users\\sample.txt").toDF("text")
        import org.apache.spark.sql.functions._
        // val df10 = df9.select(col("text"))
        val readFileDF = spark.sparkContext.textFile("C:\\Users\\sample.txt")
        val wordsDF = readFileDF.flatMap(_.split(" ")).toDF("wordcount")
        //wordsDF.show()
        val wcounts3 = wordsDF.groupBy("wordcount").count()
        //wcounts3.collect.foreach(println)
        val addingExtraColumn = df1.withColumn("D", lit(750))
        addingExtraColumn.show()
    }
}