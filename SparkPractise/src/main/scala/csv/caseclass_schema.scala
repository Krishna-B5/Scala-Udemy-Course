package csv

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

case class fschema(id:String,Date:String,num:String,category:String,prod:String,spendby:String)

object caseclass_schema {
  
  
  def main(args:Array[String]){
    
    println("====== Started =====")
    
    val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val data = sc.textFile("file:///c:/data/dt.txt")
    data.foreach(println)
    
    val splitdata = data.map(x => x.split(","))
    
    val mapdata = splitdata.map(x => fschema(x(0),x(1),x(2),x(3),x(4),x(5)))
    
    mapdata.foreach(println)
    
    val result = mapdata.filter(x => x.category.contains("Gymnastics"))
    result.foreach(println)
    
    val df = mapdata.toDF()
    df.show()
    
    val resdata = df.filter(col("category") === "Gymnastics")
    resdata.show()
        
    
  }
}