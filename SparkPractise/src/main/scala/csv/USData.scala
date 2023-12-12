package csv

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._


object USData {
  
  def main(args:Array[String]):Unit={
    
    println("======= Started =======")
    
    val conf = new SparkConf().setAppName("master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val df = spark.read.format("CSV")
             .option("header", true)
             .load("file:///c:/data/usdata.csv")
             
    df.show()
    
    println("====== Displaying only First_Name and Last_Name column =========")
    
    val pdf = df.select(col("first_name"),
                        col("last_name"))
                        
    pdf.show()
    
    
    
    println("======= END =========")
    
    
  }
  
}