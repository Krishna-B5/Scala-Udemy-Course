package csv

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object file {

	def main(args: Array[String]):Unit={

			println("Reading a CSV file")

			val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
			val sc = new SparkContext(conf)
			sc.setLogLevel("Error")

			val spark = SparkSession.builder().getOrCreate()
			import spark.implicits._

			val df = spark.read.format("csv")
			         .option(header, true)
			         .load("C:/data/CSV/addresses.csv")
			df.show()

	}
}