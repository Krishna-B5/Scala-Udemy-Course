package csv

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._


import java.text.Format

object fill {
  
  def main(args: Array[String]):Unit={
    
    println("Filtering the data")
    
    val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    println("Displaying the data")
    
    val df = spark.read.format("csv")
             .option("header", "true")
             .option("delimiter", ",")
             .load("C:/data/CSV/samplecsv.csv")
    df.show()  
    
    val df2 = df.filter("City = 'Bidar'").show()
  }
}