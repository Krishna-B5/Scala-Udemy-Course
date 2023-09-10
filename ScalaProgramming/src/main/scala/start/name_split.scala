package start

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col


object name_split {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf().setAppName("master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    println("====== Started =======")
    
//    val data = Seq(
//               ("James,A,Smith","2018","M",3000),
//               ("Robert,K,Smith","2019","M",4000)
//               )
//               
//    val df = data.toDF("Name","DOB","Gender","Salary")
//    df.printSchema()
//    df.show()
    
    val df = spark.read.format("csv").option("header", true)
            .load("file:///c:/data/Practise/name_split.csv")
            
     df.show()
    
    val resdf = df.select($"*",split($"Name",",").as("elements"))
    resdf.printSchema()
    resdf.show()
    
    resdf.withColumn("First_Name", $"elements".getItem(0))
         .withColumn("Middle_Name", $"elements".getItem(1))
         .withColumn("Last_Name", $"elements".getItem(2))
         .drop("elements")
         .show()
     
     
     
    
    println("====== End =======")
  }
}