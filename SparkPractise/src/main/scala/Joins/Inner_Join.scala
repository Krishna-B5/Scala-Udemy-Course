package Joins

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.types
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row

object Inner_Join {
  
  def main(args:Array[String]){
    
    println("====== Start =========")
    
    val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val data1 = spark.read.format("csv")
                .option("header", true)
                .load("file:///c:/data/join1.csv")
    
    data1.show()
    
    val data2 = spark.read.format("csv")
                .option("header", true)
                .load("file:///c:/data/join2.csv")
    
    data2.show()
    
    val resdf = data1.join(data2, data1("txnno") === data2("tno"), "full")
                .withColumn("txnno", expr("case when txnno is null then tno else txnno end"))
                .drop(col("tno"))
    
    resdf.show() 
    
//    val result = resdf.select(
//                              "txnno",
//                              "txndate",
//                              "amount",
//                              "custno",
//                              "spendby").orderBy(col("txnno").desc).show()
                      
    println("====== END =======")
    
  }
}