package imp

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._


object Transpose {
  
  def main(args: Array[String]):Unit={
  println("========= Started ============")
  
  val conf = new SparkConf().setAppName("first").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("Error")
  
  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._
  
  val data = Seq((1,"maths",45),
                  (1,"maths",5),
                  (1,"chemistry",43),
                  (1,"CS",42),
                  (2,"maths",35),
                  (2,"chemistry",43),
                  (2,"CS",32))
  
 
   data.foreach(println)
                  
   val df = data.toDF("RollNo","Subject","Marks")
   
   df.show()
   
   val result = df.groupBy("RollNo").pivot("Subject").agg(sum("Marks"))
                   .withColumn("Total", expr("CS+chemistry+maths") )
  
     result.show()             
   
   println("==== Done =====")
  }
}