package imp

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Column
import org.apache.spark._

object denseRank {
  
  
  def main(args:Array[String]):Unit={
    
    println("======= Started =========")
    
    val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val data1 = Seq(Row("1","krishna","1","1000"),
                   Row("2","kiran","2","2000"),
                   Row("3","kavitha","3","500"),
                   Row("4","suresh","1","600"),
                   Row("5","prem","2","800"),
                   Row("6","anurag","3","900"),
                   Row("7","takshvi","1","500"))
    
    val tschema = StructType(Array(
                  StructField("id",StringType,true),
                  StructField("name",StringType,true),
                  StructField("deptid",StringType,true),
                  StructField("salary",StringType,true)
                  ))
     
    val df = spark.createDataFrame(
             spark.sparkContext.parallelize(data1), tschema)
    df.show()
             
    val win = Window.partitionBy(col("deptid"))
                    .orderBy(col("salary").desc)
                    
    val result = df.withColumn("rank", dense_rank().over(win))
                   .orderBy("deptId")
                   
    print("Enter a number to find the highest salary: ")            
    
    var n = scala.io.StdIn.readInt()     
    println(s"The $n highest salary") 
    
    result.filter(s"rank = $n").show() 
    
    println("\n ===== Done =====")
                 
                   
  } 
}