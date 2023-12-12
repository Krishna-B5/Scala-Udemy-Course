package csv

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row._
import org.apache.spark.sql.functions._

case class schema(id:String,Date:String,prod:String,category:String)
case class tschema(prod:String,Cat:String)

object schemaRDD {
  
  def main(args: Array[String]){
    
    println("====== Started ======")
    
    val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val rdddata = sc.textFile("file:///C:/data/datatxns.txt")
    rdddata.foreach(println)
    
    println("===== Row filter =====")
    val rowfilter = rdddata.filter(x => x.contains("Gymnastics"))
    rowfilter.foreach(println)
    
    println("===== Spliting the data ========\n")
    val split1 = rdddata.map(x => x.split(","))
    
    println("====== case class schema =======\n")
    val rowdd = split1.map(x => schema(x(0),x(1),x(2),x(3)))
    val rowdd1 = split1.map(x => tschema(x(1),x(3)))
//    rowdd.foreach(println)   
//    rowdd1.foreach(println)
    
    println("===== Column Based filter =======")
    val colfilter = rowdd.filter(x => x.category.contains("Gymnastics"))
    colfilter.foreach(println)
 
//    println("======= converting to DF ========")
//    
//    val df = rowdd.toDF()
//    df.show()
//    df.printSchema()
    
  }
}