package com.kennyudovic.sparkScalaInterview

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

//Completing the task without SQL
object tableDifferences {
  def main(args: Array[String]) {
    // Set the Logger Level for reports
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Construct the Spark Context
    val spark = SparkSession
      .builder
      .appName("TableDifferences")
      .master("local[*]")
      .getOrCreate

    // This could be made into a loop to process more tables
    val df1 = spark 
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .option("inferSchema", "true")
    .csv(args(0))

    val df2 = spark 
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .option("inferSchema", "true")
    .csv(args(1))
    
    // Cache to make operations faster
    df1.cache()
    df2.cache()

    // For debugging purposes
    //df1.show()
    //df2.show()

    //import spark.implicits._
    import org.apache.spark.sql.functions._

    val columnNames = df1.schema.fields.map(_.name)

    //println("No Change Rows")
    val noChange = df1.intersect(df2) //.orderBy(asc(columnNames(0)))
    val noChangeFlagged = noChange.withColumn("flag", lit("No Change"))
    //noChangeFlagged.show()

    //println("Deleted Rows")
    val deleted = df1.join(df2, Seq(columnNames(0)), "leftanti") //.orderBy(asc(columnNames(0)))
    val deletedFlagged = deleted.withColumn("flag", lit("Deleted"))
    //deletedFlagged.show()
    
    //println("Added Rows")
    val added = df2.join(df1, Seq(columnNames(0)), "leftanti") //.orderBy(asc(columnNames(0)))
    val addedFlagged = added.withColumn("flag", lit("Added"))
    //addedFlagged.show()

    //println("Updated Rows")
    val updated = df2.join(df1, Seq(columnNames(0)), "leftsemi").except(df1) //.orderBy(asc(columnNames(0)))
    val updatedFlagged = updated.withColumn("flag", lit("Updated"))
    //updatedFlagged.show()

    val finalResult = updatedFlagged.union(addedFlagged.union(deletedFlagged.union(noChangeFlagged))).orderBy(asc(columnNames(0)))

    val results = finalResult.collect()
    results.foreach(println)

    //results.saveAsTextFile("output/final.csv")
  }
}



// Object to display SQL queries in Spark
object tableDifferencesSQL {

  def main(args: Array[String]) {
    // Set the Logger Level for reports
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Construct the Spark Context
    val spark = SparkSession
      .builder
      .appName("TableDifferences")
      .master("local[*]")
      .getOrCreate

    // This could be made into a loop to process more tables
    val df1 = spark 
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .option("inferSchema", "true")
    .csv(args(0))

    val df2 = spark 
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .option("inferSchema", "true")
    .csv(args(1))
    
    // Cache to make operations faster
    df1.cache()
    df2.cache()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    df1.createOrReplaceTempView("tableOne")
    df2.createOrReplaceTempView("tableTwo")

    val columnNames = df1.schema.fields.map(_.name)
    //println("No Change Rows ")
    val noChange = spark.sql("SELECT * FROM tableOne INTERSECT SELECT * FROM tableTwo")
    //noChange.show()

    //println("Deleted Rows")
    val deleted = spark.sql("SELECT * FROM tableOne WHERE " + columnNames(0) + " NOT IN (SELECT " + columnNames(0) + " FROM tableTwo)") //.orderBy(asc(columnNames(0)))
    //deleted.show()
    
    //println("Added Rows")
    val added = spark.sql("SELECT * FROM tableTwo WHERE " + columnNames(0) + " NOT IN (SELECT " + columnNames(0) + " FROM tableOne)") //.orderBy(asc(columnNames(0)))
    //added.show()

    //println("Updated Rows")
    val updated = spark.sql("SELECT * FROM tableTwo WHERE EXISTS (SELECT 1 FROM tableOne WHERE tableTwo." + columnNames(0) + " = tableOne." + columnNames(0)  + ") EXCEPT (SELECT * FROM tableOne)")
    //updated.show()

    val noChangeFlagged = noChange.withColumn("flag", lit("No Change"))
    val deletedFlagged = deleted.withColumn("flag", lit("Deleted"))
    val addedFlagged = added.withColumn("flag", lit("Added"))
    val updatedFlagged = updated.withColumn("flag", lit("Updated"))

    val finalResult = updatedFlagged.union(addedFlagged.union(deletedFlagged.union(noChangeFlagged))).orderBy(asc(columnNames(0)))

    val results = finalResult.collect()
    results.foreach(println)

    //results.saveAsTextFile("output/final.csv")
  }
}
