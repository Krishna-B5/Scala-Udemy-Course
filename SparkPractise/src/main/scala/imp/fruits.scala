package imp

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._



object fruits {
  
// WAP to groupby date transpose the fruits and find the total
	println("====== This is telegram example started =======")

	def main(args: Array[String]):Unit={

			val spark = SparkSession.builder().appName("master").master("local[*]").getOrCreate()
					import spark.implicits._
					spark.sparkContext.setLogLevel("ERROR")

					val data = Seq(Row("18-11-2022","apple",10),
							Row("18-11-2022","orange",8),
							Row("19-11-2022","apple",5),
							Row("19-11-2022","orange",5),
							Row("17-11-2022","apple",7),
							Row("17-11-2022","orange",10))

					val tschema = StructType(Array(
							StructField("Date",StringType,true),
							StructField("Fruit",StringType,true),
							StructField("count",IntegerType,true)
							))

					val df = spark.createDataFrame(
							spark.sparkContext.parallelize(data), tschema)

					df.show()

					val resdf = df.groupBy("Date").pivot("Fruit").agg(sum("count"))
					.withColumn("Difference", expr("apple-orange") )
					.drop("apple","orange")
					resdf.show()


					println("========= Done ===========")

	}
}
