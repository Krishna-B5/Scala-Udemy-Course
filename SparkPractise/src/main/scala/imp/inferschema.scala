package imp

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.Dataset

object inferschema {
  
  def main(args:Array[String]):Unit={
    
    println("====== Started =======")
    
    val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
			val sc = new SparkContext(conf)
			sc.setLogLevel("Error")
			
			val spark = SparkSession.builder().getOrCreate()
			import spark.implicits._
			
			println("======== With Inferschema =============")
			
			val df = spark.read.format("csv")
			         .option("header", "true")
			         .option("delimiter", "|")
			         .option("inferschema", "true") 
			         .load("C:/data/CSV/dense_rank.csv")
			         
		 df.show()
		 df.printSchema()
		 
		 println("======== Without Inferschema =============")
		 
		 val df1 = spark.read.format("csv")
			         .option("header", "true")
			         .option("delimiter", "|")
			         .load("C:/data/CSV/dense_rank.csv")
			         
		 df1.show()
		 df1.printSchema()
  }
  
}