package csv

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.Dataset


object file {

	def main(args: Array[String]):Unit={

			println("Dense program from telegram group")
			
			val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
			val sc = new SparkContext(conf)
			sc.setLogLevel("Error")
			
			val spark = SparkSession.builder().getOrCreate()
			import spark.implicits._
			
			println("Reading a CSV file")
			
			val df = spark.read.format("csv")
			         .option("header", "true")
			         .option("delimiter", "|")
			         .option("inferschema", "true") 
			         .load("C:/data/CSV/dense_rank.csv")	
			      
			 df.show()
			val df2 = df.filter("Product_Categroy = 'bathroom'")
			df2.show()
			         
				
		  val windowSpe = Window.partitionBy("Product_Categroy").orderBy(col("Sale_Amount").desc)
		  
			println("======= Rank returns sequential number with gaps ======")        
			val df1 = df.withColumn("Rank", rank().over(windowSpe)).show()
			
			println("======= Dense rank returns number without any gaps ========")
			val df3 = df.withColumn("Rank", dense_rank().over(windowSpe)).show()
			
			println("====== Row Number returns seq of num starting with 1 =======")
			val df4 = df.withColumn("Rank", row_number().over(windowSpe)).show()
			
			println("====== Percentile Returns percentage rank of rows ========")
		  val df5 = df.withColumn("Rank", percent_rank().over(windowSpe)).show()



	}
} 