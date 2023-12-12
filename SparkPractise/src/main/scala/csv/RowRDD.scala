package csv

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object RowRDD {
  
 def main(args: Array[String]){
   
   println("===== Started =====")
   
   val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
   val sc = new SparkContext(conf)
   sc.setLogLevel("Error")
   
   val spark = SparkSession.builder().getOrCreate()
   import spark.implicits._
    
   val rdddata = sc.textFile("file:///c:/data/datatxns.txt")
   rdddata.foreach(println)
   
   val rddsplit = rdddata.map(x => x.split(","))
   val imposerdd = rddsplit.map(x => Row(x(0)))
   
   imposerdd.foreach(println)
   
   val schema = StructType(Array(
               StructField("id",StringType,true)
               ))
               
   val df = spark.createDataFrame(imposerdd, schema)
   df.show()
   df.printSchema()
   
 }
}