package csv

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row._
import org.apache.spark.sql._
import org.apache.spark.sql.functions
import java.text.Format

object DT_File {

	def main(args:Array[String]){

		println("======= Started ========")

		val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")

		val spark = SparkSession.builder().getOrCreate()
		import spark.implicits._

		println("======= RDD Data =======")
		val rddddata = sc.textFile("file:///c:/data/dt.txt")
		rddddata.foreach(println)

		val tschema = StructType(Array(
				StructField("id",StringType,true),
				StructField("tdate",StringType,true),
				StructField("amount",StringType,true),
				StructField("category",StringType,true),
				StructField("product",StringType,true),
				StructField("spendby",StringType,true)
				))
				
	 
		val rdata = rddddata.map( x => x.split(",")) // U can't print Array[String] values
		
		rdata.foreach(println)


		println("======== Original Data without header========")    
		val data = spark.read.format("csv")
		           .option("delimiter",",")
		           .load("file:///c:/data/dt.txt")      

		data.show()

		println("=========== Applying header Struct to DF spark read ===========")

		val data1 = spark.read.format("csv")
		            .schema(tschema)
		            .load("file:///c:/data/dt.txt")

		data1.show()
		println("\n========== Seq Data ==========")
		val seqdata = Seq(Row("00000000","06-26-2011","200","Exercise","GymnasticsPro","cash"),
		                  Row("00000003","06-05-2011","100","Gymnastics","Rings",""))

		seqdata.foreach(println)
		
		println("\n ========== Reading Seq data & converting to DF using parllelize")
		
		val df = spark.createDataFrame(
		          spark.sparkContext.parallelize(seqdata),tschema)
		          
		 df.show()
		          
		println(" ======== END =========")


	}
}